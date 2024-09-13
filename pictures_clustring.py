import time
import os
import shutil
import pickle
from functools import wraps
import click
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from geopy.geocoders import Photon
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderServiceError
from datetime import datetime
from alive_progress import alive_bar
from pymp4.parser import Box
from slugify import slugify

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

# Example
# {'address_components': [{'long_name': 'XPJV+5P',
#   'short_name': 'XPJV+5P',
#   'types': ['plus_code']},
#  {'long_name': 'Rishon LeTsiyon',
#   'short_name': 'ראשל"צ',
#   'types': ['locality', 'political']},
#  {'long_name': 'Rehovot',
#   'short_name': 'Rehovot',
#   'types': ['administrative_area_level_2', 'political']},
#  {'long_name': 'Center District',
#   'short_name': 'Center District',
#   'types': ['administrative_area_level_1', 'political']},
#  {'long_name': 'Israel',
#   'short_name': 'IL',
#   'types': ['country', 'political']}],
# 'formatted_address': 'XPJV+5P, Rishon LeTsiyon, Israel',
# 'geometry': {'location': {'lat': 31.98052149999999, 'lng': 34.7452179},
#  'location_type': 'GEOMETRIC_CENTER',
#  'viewport': {'northeast': {'lat': 31.98187048029149,
#    'lng': 34.7465668802915},
#   'southwest': {'lat': 31.9791725197085, 'lng': 34.7438689197085}}},
# 'place_id': 'ChIJVfheG_qzAhURGGd62_CnTh8',
# 'types': ['establishment', 'point_of_interest']}
# -----
# {'address_components': [{'long_name': '8G3QM39P+6P',
#   'short_name': '8G3QM39P+6P',
#   'types': ['plus_code']}],
# 'formatted_address': '8G3QM39P+6P',
# 'geometry': {'bounds': {'northeast': {'lat': 31.668125, 'lng': 35.086875},
#   'southwest': {'lat': 31.668, 'lng': 35.08674999999999}},
#  'location': {'lat': 31.6681, 'lng': 35.0868139},
#  'location_type': 'GEOMETRIC_CENTER',
#  'viewport': {'northeast': {'lat': 31.6694114802915, 'lng': 35.0881614802915},
#   'southwest': {'lat': 31.6667135197085, 'lng': 35.0854635197085}}},
# 'place_id': 'GhIJJnUCmgirP0ARSqvGtxyLQUA',
# 'plus_code': {'global_code': '8G3QM39P+6P'},
# 'types': ['plus_code']}
def get_city_from_address(address):
    if 'address_components' in address:
        if address['types'] == ['plus_code']:
            return 'Unknown'
        address = address['address_components']
        for component in address:
            if 'locality' in component.get('types', []):
                return component['long_name']
        else:
            return 'Unknown'
    address = address.get('properties', address)
    address = address.get('address', address)
    if 'city' in address:
        return address['city']
    if 'town' in address:
        return address['town']
    if 'village' in address:
        return address['village']
    return 'Unknown'

def get_country_from_address(address):
    if 'address_components' in address:
        if address['types'] == ['plus_code']:
            return 'Unknown'
        address = address['address_components']
        for component in address:
            if 'country' in component.get('types', []):
                return component['long_name']
        else:
            return 'Unknown'
    address = address.get('properties', address)
    address = address.get('address', address)
    if 'country' in address:
        return address['country']
    return 'Unknown'


location_cache = {}
def get_location(geotags, image_path, geo_service):
    if not geotags:
        print(f"No GEOTagging for picture {image_path}")
        return "Unknown", "Unknown"
    if 'GPSLatitude' not in geotags or 'GPSLongitude' not in geotags:
        print(f"GEO tags missing coordinates {image_path}")
        return "Unknown", "Unknown"

    lat, lon = get_coordinates(geotags)
    print(f"Query of {lat}, {lon}")

    # Check cache for existing result
    cache_key = f"{lat:.6f},{lon:.6f}"
    if cache_key in location_cache:
        return location_cache[cache_key]

    # If not in cache, query the geolocation service
    for attempt in range(10):
        try:
            if 'type1' == geo_service:
                geolocator = Geocodio('07d9aed7dde306690474b3aedd5de5c9ae5e643', user_agent='Python')
                location = geolocator.reverse(f'{lat}, {lon}', exactly_one=True) #, language='en')
            elif 'type2' == geo_service:
                geolocator = Photon(user_agent='Python')
                location = geolocator.reverse((lat, lon), exactly_one=True)
            elif 'type3' == geo_service:
                geolocator = GoogleV3('AIzaSyDiWErdHbLL2GN02yGnN-OsNWyl2VnY6FY')
                location = geolocator.reverse(f'{lat}, {lon}') #, language='English')
            else:
                raise Exception(f"Invalid geo service {geo_service}")
            if location is None:
                print(f"Query failed for {lat}, {lon}")
                #return 'Unknown', 'Unknown'
                raise Exception(f"Query failed for {(lat, lon)}")
            break
        except GeocoderServiceError as e:
            print(f"Error in {attempt}")
            print(repr(e))
            time.sleep(1)
    else:
        raise Exception("Geo location query failed")

    address = location.raw

    # Extract city and country names
    city = get_city_from_address(address)
    country = get_country_from_address(address)

    # Store the result in cache
    location_cache[cache_key] = (country, city)

    # Print the location since it's a cache miss
    print(f"New location queried: {country}, {city} for {lat}, {lon}")

    return country, city


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
def get_metadata(file_path, geo_service):
    if file_path.lower().endswith(('.png', '.jpg', '.jpeg')):
        image = Image.open(file_path)
        exif = image._getexif()
        geotags = get_geotagging(exif)
        country, city = get_location(geotags, file_path, geo_service)
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
@click.option('--geo_service', type=click.Choice(['type1', 'type2', 'type3']), default='google', required=False)
def cluster_images(input_directory, output_directory, geo_service):
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
            country, city, date = get_metadata(file_path, geo_service)
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


