import codecs
import csv
import subprocess
import geoip2.database
import ray
import json
import numpy as np
from progress.bar import Bar
from device_detector import DeviceDetector
from pprint import pprint

# This creates a Reader object. You should use the same object
# across multiple requests as creation of it is expensive.
geoip = geoip2.database.Reader('./GeoLite2/GeoLite2-City.mmdb')

# Initialize ray
ray.init()

@ray.remote
def fetch_row_data(row):
    row = json.loads(row)
    # Merge data into single record.
    row.update(getDeviceData(row['request_user_agent']))
    row.update(getIpData(row['ip']))
    return row

@ray.remote
def write_row_data(writer, row):
    # Add to file.
    writer.writerow(row)
    # Increment progress bar
    bar.next()
    return "written"

# Extract GeoIP data
def getIpData(ip):

    # response.country.iso_code 'US'
    # response.country.name 'United States'
    # response.subdivisions.most_specific.name 'Minnesota'
    # response.subdivisions.most_specific.iso_code 'MN'
    # response.city.name 'Minneapolis'
    # response.postal.code '55455'
    # response.location.latitude 44.9733
    # response.location.longitude -93.2323
    # response.traits.network IPv4Network('128.101.101.0/24')
    split_ip = splitIp(ip)

    try:
        # Replace "city" with the method corresponding to the database
        # that you are using, e.g., "country".
        response = geoip.city(ip)

        data = {
        "city": response.city.name,
        "country": response.country.iso_code,
        "latitude": response.location.latitude,
        "longitude": response.location.longitude,
        "split_ip": split_ip
    }
    except:
        data = {"city": "","country": "","latitude": "","longitude": "", "split_ip": split_ip}

    return data

# Get user agent data
def getDeviceData(ua):
    device = DeviceDetector(ua).parse()
    data = {
        "device_brand": device.device_brand(),
        "device_model": device.device_model(),
        "device_type": device.device_type(),
        "os_name": device.os_name(),
        "os_version": device.os_version(),
        "client_name": device.client_name(),
        "client_type": device.client_type(),
        "client_version": device.client_version()
    }

    return data

# Split IP address
def splitIp(ip):
    if (len(ip) > 39):
        arr = ip.split(':')
        txt = arr[0]+"."+arr[1]+":0000:0000:0000:0000:0000:0000"
    else:
        arr = ip.split('.')
        txt = arr[0]+"."+arr[1]+".0.0"
    return txt

def line_count(filename):
    return int(subprocess.check_output('wc -l {}'.format(filename), shell=True).split()[0])

### MAIN SCRIPT
# Define file input/output
IN_FILENAME = "02-20-2020-output.csv"
OUT_FILENAME = "02-20-2020-complete.csv"
ENCODING = 'utf-8'

# Get lines and create progress bar
lc = line_count(IN_FILENAME)
<<<<<<< HEAD
bar = Bar('Processing', max=lc, suffix = '%(percent).1f%% | [%(index)d/%(max)d] | %(eta)ds')
=======
bar = Bar('Processing', max=lc, suffix = '%(percent).1f%% / [%(index)/%(max)] / %(eta)ds')
>>>>>>> Attempt using Ray

# Open CSVs for reading / writing
with codecs.open(IN_FILENAME, "r", ENCODING) as rp:
    reader = csv.DictReader(rp)
    with codecs.open(OUT_FILENAME, "w", ENCODING) as wp:

        # read CSV headers
        headers = next(reader)
        headers.update(getIpData(headers['ip']))
        headers.update(getDeviceData(headers['request_user_agent']))

        # Set up writer
        writer = csv.DictWriter(wp, fieldnames=headers)
        
        # Write headers and initial record to CSV
        writer.writeheader()
        writer.writerow(headers)

        # Read rest of file
        for i, row in enumerate(reader):
            # Pass data to multiprocessing threads
            nrow = json.dumps(row)
            row_data = fetch_row_data.remote(nrow)
            write_row_data(writer, row_data)
        
        # Get the results.
        # z = ray.get(z_id)
    
geoip.close()
bar.finish()