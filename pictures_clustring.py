import time
import os
import shutil
import pickle
from functools import wraps
import click
from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS
from geopy.geocoders import Nominatim, Photon, GoogleV3, ArcGIS, OpenCage
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
            if not CACHE_DB:
                if os.path.exists(cache_file):
                    with open(cache_file, 'rb') as f:
                        CACHE_DB = pickle.load(f)
                else:
                    CACHE_DB = {}
            key = repr((args, kwargs))
            if key in CACHE_DB:
                return CACHE_DB[key]

            result = func(*args, **kwargs)
            CACHE_DB[key] = result

            with open(cache_file, 'wb') as f:
                pickle.dump(CACHE_DB, f)

            return result
        return wrapper
    return decorator

def get_geotagging(exif):
    if not exif:
        return None

    geotagging = {}
    for (idx, tag) in TAGS.items():
        if tag == 'GPSInfo':
            if idx not in exif:
                return None

            for (key, val) in GPSTAGS.items():
                if key in exif[idx]:
                    geotagging[val] = exif[idx][key]

    return geotagging

@cache("geo_coordinate_cache.pkl")
def get_coordinates(geotags):
    lat = lon = None
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

def get_city_from_address(address):
    if isinstance(address, dict):
        if 'address' in address:
            address = address['address']
        return (address.get('city') or address.get('town') or
                address.get('village') or address.get('hamlet') or 'Unknown')
    elif hasattr(address, 'raw'):
        raw = address.raw
        if 'address' in raw:
            components = raw['address']
            return (components.get('city') or components.get('town') or
                    components.get('village') or components.get('hamlet') or 'Unknown')
    return 'Unknown'

def get_country_from_address(address):
    if isinstance(address, dict):
        if 'address' in address:
            address = address['address']
        return address.get('country', 'Unknown')
    elif hasattr(address, 'raw'):
        raw = address.raw
        if 'address' in raw:
            return raw['address'].get('country', 'Unknown')
    return 'Unknown'

@cache("location_cache.pkl")
def get_location(geotags, image_path, geo_service, api_key=None):
    if not geotags or 'GPSLatitude' not in geotags or 'GPSLongitude' not in geotags:
        print(f"No valid GEOTagging for picture {image_path}")
        return "Unknown", "Unknown"

    lat, lon = get_coordinates(geotags)
    print(f"Query of {lat}, {lon}")

    geolocators = {
        'nominatim': lambda: Nominatim(user_agent="MyApp"),
        'photon': lambda: Photon(user_agent="MyApp"),
        'google': lambda: GoogleV3(api_key='AIzaSyDiWErdHbLL2GN02yGnN-OsNWyl2VnY6FY'),
        'arcgis': lambda: ArcGIS(),
        'opencage': lambda: OpenCage(api_key=api_key),
        'geocodio': lambda: Geocodio('07d9aed7dde306690474b3aedd5de5c9ae5e643', user_agent='Python')
    }

    if geo_service not in geolocators:
        raise ValueError(f"Invalid geo service {geo_service}")

    geolocator = geolocators[geo_service]()

    for attempt in range(3):
        try:
            location = geolocator.reverse(f"{lat}, {lon}")

            if location is None:
                print(f"Query failed for {lat}, {lon}")
                raise Exception(f"Query failed for {(lat, lon)}")

            city = get_city_from_address(location)
            country = get_country_from_address(location)
            print(f"New location queried: {country}, {city} for {lat}, {lon}")
            return country, city
        except GeocoderServiceError as e:
            print(f"Error in attempt {attempt + 1}: {repr(e)}")
            time.sleep(2)

    print(f"All attempts failed for {lat}, {lon}")
    return "Unknown", "Unknown"

def get_file_datetime(file_path):
    return datetime.fromtimestamp(os.path.getmtime(file_path)).date()

def get_date_taken(exif, file_path, is_video=False):
    if is_video:
        with open(file_path, "rb") as f:
            boxes = Box.parse_stream(f)
            for box in boxes:
                if hasattr(box, 'type') and box.type.decode("utf-8") == "mvhd":
                    return datetime.fromtimestamp(box.creation_time - 2082844800).date()
    elif exif and 36867 in exif:
        date = exif[36867]
        return datetime.strptime(date, "%Y:%m:%d %H:%M:%S").date()
    return get_file_datetime(file_path)

@cache("media_indexing.pkl")
def get_metadata(file_path, geo_service, api_key):
    lower_path = file_path.lower()
    if lower_path.endswith(('.png', '.jpg', '.jpeg')):
        with Image.open(file_path) as image:
            exif = image._getexif()
        if exif:
            geotags = get_geotagging(exif)
            country, city = get_location(geotags, file_path, geo_service, api_key)
            return country, city, get_date_taken(exif, file_path)
    elif lower_path.endswith('.mp4'):
        return None, None, get_date_taken(None, file_path, is_video=True)
    return None, None, None

def do_files(files_list, start_date, end_date, output_directory, country, city):
    if start_date is None or end_date is None:
        return

    date_range = (f"{start_date.strftime('%Y-%m-%d')}" if start_date == end_date
                  else f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}")

    location = '_'.join(filter(None, [country, city]))
    subdir = '_'.join(filter(None, [date_range, location]))
    subdir = slugify(subdir)

    new_directory = os.path.join(output_directory, subdir)
    os.makedirs(new_directory, exist_ok=True)

    for media_file in files_list:
        shutil.copy2(media_file, new_directory)

@click.command()
@click.argument('input_directory', type=click.Path(exists=True))
@click.argument('output_directory', type=click.Path())
@click.option('--geo_service', type=click.Choice(['nominatim', 'photon', 'google', 'arcgis', 'opencage', 'geocodio']),
              default='nominatim', help='Geolocation service to use')
@click.option('--api_key', help='API key for the chosen geolocation service (if required)')
def cluster_images(input_directory, output_directory, geo_service, api_key):
    files_list = [os.path.join(input_directory, f) for f in os.listdir(input_directory)
                  if f.lower().endswith(('.png', '.jpg', '.jpeg', '.mp4'))]
    files_list.sort()

    print(f"Start indexing {len(files_list)} files")
    with alive_bar(len(files_list), title="Indexing") as bar:
        image_data = []
        for file_path in files_list:
            country, city, date = get_metadata(file_path, geo_service, api_key)
            if date is not None:
                image_data.append((file_path, country or "Unknown", city or "Unknown", date))
            bar()

    image_data.sort(key=lambda x: x[3])

    print(f"Start processing {len(image_data)} files")
    with alive_bar(len(image_data), title="Processing") as bar:
        current_group = []
        prev_country, prev_city, start_date = image_data[0][1:4]

        for image_path, country, city, date in image_data:
            if country != prev_country or city != prev_city:
                do_files(current_group, start_date, prev_date, output_directory, prev_country, prev_city)
                current_group = []
                prev_country, prev_city, start_date = country, city, date

            current_group.append(image_path)
            prev_date = date
            bar()

        # Handle the last group
        do_files(current_group, start_date, prev_date, output_directory, prev_country, prev_city)

if __name__ == "__main__":
    cluster_images()
