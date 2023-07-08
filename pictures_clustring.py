import os
import shutil
import pickle
from functools import wraps
import click
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from geopy.geocoders import Nominatim
from datetime import datetime
from alive_progress import alive_bar
from pymp4.parser import Box

CACHE_DB = None
def cache(cache_file):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global CACHE_DB
            # Check if cache file exists
            if not CACHE_DB:
                if os.path.exists(cache_file):
                    # Load cached data from file
                    with open(cache_file, 'rb') as f:
                        CACHE_DB = pickle.load(f)
                else:
                    CACHE_DB = {}
            key = repr((args, kwargs))
            if key in CACHE_DB:
                return CACHE_DB[key]

            # Cache file doesn't exist, call the decorated function
            result = func(*args, **kwargs)
            CACHE_DB[key] = result

            # Save the result to cache file
            with open(cache_file, 'wb') as f:
                pickle.dump(CACHE_DB, f)

            return result

        return wrapper

    return decorator

def get_geotagging(exif):
    if not exif:
        return None
        # raise ValueError("No EXIF metadata found")

    geotagging = {}
    for (idx, tag) in TAGS.items():
        if tag == 'GPSInfo':
            if idx not in exif:
                return None
                # raise ValueError("No EXIF geotagging found")

            for (key, val) in GPSTAGS.items():
                if key in exif[idx]:
                    geotagging[val] = exif[idx][key]

    return geotagging

@cache("geo_coordinate_cache.pkl")
def get_coordinates(geotags):
    lat = None
    lon = None

    # Convert latitude and longitude to degrees format
    for tag, value in geotags.items():
        if tag == 'GPSLatitude':
            lat = sum([float(x) / 60 ** n for n, x in enumerate(value)])
            if geotags.get('GPSLatitudeRef') == 'S':
                lat = -lat
        elif tag == 'GPSLongitude':
            lon = sum([float(x) / 60 ** n for n, x in enumerate(value)])
            if geotags.get('GPSLongitudeRef') == 'W':
                lon = -lon

    return lat, lon

location_cache = {}
def get_location(geotags, image_path):
    if not geotags:
        print(f"No GEOTagging for picture {image_path}")
        return "Unknown", "Unknown"
    if 'GPSLatitude' not in geotags or 'GPSLongitude' not in geotags:
        print(f"GEO tags missing coordinates {image_path}")
        return "Unknown", "Unknown"

    try:
        lat, lon = get_coordinates(geotags)

        # Check cache for existing result
        cache_key = f"{lat:.6f},{lon:.6f}"
        if cache_key in location_cache:
            return location_cache[cache_key]

        # If not in cache, query the geolocation service
        geolocator = Nominatim(user_agent="geoapiExercises")
        location = geolocator.reverse((lat, lon), exactly_one=True, language='en')
        address = location.raw['address']

        # Extract city and country names
        city = address.get('city', '')
        if not city:
            city = address.get('town', '')
        if not city:
            city = address.get('village', '')
        country = address.get('country', '')

        # Store the result in cache
        location_cache[cache_key] = (country, city)

        # Print the location since it's a cache miss
        print(f"New location queried: {country}, {city}")

        return country, city

    except (AttributeError, KeyError, IndexError, TypeError):
        return "Unknown", "Unknown"


def get_file_datetime(file_path):
    timestamp = os.path.getmtime(file_path)
    return datetime.fromtimestamp(timestamp).date()

def get_date_taken(exif, file_path, is_video=False):
    if is_video:
        # If it's video, read metadata for creation date
        with open(file_path, "rb") as f:
            boxes = Box.parse_stream(f)
            for box in boxes:
                if hasattr(box, 'type') and box.type.decode("utf-8") == "mvhd":
                    return datetime.fromtimestamp(box.creation_time - 2082844800).date()
    elif 36867 in exif:
        # Try to get date from EXIF data (36867 is the tag for DateTimeOriginal)
        date = exif[36867]
        return datetime.strptime(date, "%Y:%m:%d %H:%M:%S").date()

    # If no EXIF date taken available, use the file timestamp
    return get_file_datetime(file_path)

@cache("media_indexing.pkl")
def get_metadata(file_path):
    if file_path.lower().endswith(('.png', '.jpg', '.jpeg')):
        image = Image.open(file_path)
        exif = image._getexif()
        geotags = get_geotagging(exif)
        country, city = get_location(geotags, file_path)
        return country, city, get_date_taken(exif, file_path)
    elif file_path.lower().endswith('.mp4'):
        return None, None, get_file_datetime(file_path)
    return None, None, None

def do_files(proc, files_list, start_date, end_date, output_directory, country, city):
    if None == start_date:
        return
    if None == end_date:
        return
    if start_date == end_date:
        date_range = f"{start_date.strftime('%Y-%m-%d')}"
    else:
        date_range = f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"
    if country and city:
        subdir = '_'.join([date_range, country, city])
    elif country:
        subdir = '_'.join([date_range, country])
    else:
        subdir = date_range
    new_directory = os.path.join(output_directory, subdir)
    os.makedirs(new_directory, exist_ok=True)
    for media_file in files_list:
        shutil.copy2(media_file, new_directory)

@click.command()
@click.argument('input_directory', type=click.Path(exists=True))
@click.argument('output_directory', type=click.Path())
def cluster_images(input_directory, output_directory):
    image_data = []
    files_list = os.listdir(input_directory)
    files_list.sort()

    last_country = 'Unknown'
    last_city = 'Unknown'
    # Extract metadata and store in image_data
    print(f"Start indexing {len(files_list)} files")
    with alive_bar(len(files_list), title="Indexing") as bar:
        for media_file in files_list:
            file_path = os.path.join(input_directory, media_file)
            country, city, date = get_metadata(file_path)
            bar()
            if None == date:
                print(f"Skipping file {media_file}")
                continue
            country = country or last_country
            city = city or last_city
            last_country = country
            last_city = city
            image_data.append((file_path, country, city, date))
    print("Done indexing")

    # Sort image_data by date
    image_data.sort(key=lambda x: x[3])

    # Create directories and move images
    first = image_data[0]
    prev_country = first[1]
    prev_city = first[2]
    prev_date = first[3]
    start_date = first[3]
    end_date = None
    copy_list = []

    print(f"Start processing {len(image_data)} files")
    with alive_bar(len(image_data), title="Processing") as bar:
        for image_path, country, city, date in image_data:
            if country == prev_country and city == prev_city:
                copy_list.append(image_path)
            else:
                do_files(shutil.copy2, copy_list, start_date, prev_date, output_directory, prev_country, prev_city)
                copy_list = [image_path]
                prev_country = country
                prev_city = city
                start_date = date
            prev_date = date
            bar()

        # Handle the last group
        do_files(shutil.copy2, copy_list, start_date, prev_date, output_directory, prev_country, prev_city)


if __name__ == "__main__":
    cluster_images()


