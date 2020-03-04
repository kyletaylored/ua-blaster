#!/bin/python3

import click
import subprocess
import geoip2.database
import time
from dask.diagnostics import ProgressBar
from device_detector import DeviceDetector
import dask.dataframe as dd
from netaddr import IPAddress
from pprint import pprint # Debug support

# This creates a Reader object. You should use the same object
# across multiple requests as creation of it is expensive.
geoip = geoip2.database.Reader('./GeoLite2/GeoLite2-City.mmdb')

# Initialize progress bar
pbar = ProgressBar()

# Extract GeoIP data
def getIpData(ip: str):

    # response.country.iso_code 'US'
    # response.country.name 'United States'
    # response.subdivisions.most_specific.name 'Minnesota'
    # response.subdivisions.most_specific.iso_code 'MN'
    # response.city.name 'Minneapolis'
    # response.postal.code '55455'
    # response.location.latitude 44.9733
    # response.location.longitude -93.2323
    # response.traits.network IPv4Network('128.101.101.0/24')

    try:
        # Replace "city" with the method corresponding to the database
        # that you are using, e.g., "country".
        response = geoip.city(ip)

        # data = {
        #     "city": response.city.name,
        #     "country": response.country.iso_code,
        #     "latitude": response.location.latitude,
        #     "longitude": response.location.longitude,
        # }
        data = [response.city.name,response.country.iso_code,response.location.latitude,response.location.longitude]
    except:
        # data = {"city": "","country": "","latitude": "","longitude": ""}
        data = ["","","",""]
   
    return data

# Get user agent data
def getDeviceData(ua):
    ua = str(ua)
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
    
    return list(data.values())

# Split IP address
def splitIp(ip: str):
    ipc = IPAddress(ip)
    if (ipc.version == 6):
        arr = ip.split(':')
        txt = arr[0]+":"+arr[1]+":0000:0000:0000:0000:0000:0000"
    else:
        arr = ip.split('.')
        txt = arr[0]+"."+arr[1]+".0.0"
    return txt

# Get dataframe metadata
def getMeta(df):
    meta = list(df.head(1).to_dict().keys())
    return list_to_dict(meta)

def list_to_dict(the_list):
    return dict(map( lambda x: (x, 'object'), the_list))

# Add GeoIP data to dataframe
def processIpDataframe(df, meta):
    cols = ["city","country","latitude","longitude"]
    df[cols] = df.apply(lambda x: getIpData(x.ip), axis=1, result_type="expand", meta=meta)
    
    meta.update(list_to_dict(cols))
    return meta

# Add split IP data to dataframe
def processSplitIpDataframe(df, meta):
    cols = ["split_ip"]
    df[cols] = df.apply(lambda x: splitIp(x.ip), axis=1, meta=meta)

    meta.update(list_to_dict(cols))
    return meta

# Add device info to dataframe
def processUserAgentDataframe(df, meta):
    cols = ["device_brand","device_model","device_type","os_name","os_version","client_name","client_type","client_version"]
    df[cols] = df.apply(lambda x: getDeviceData(x.request_user_agent), axis=1, result_type="expand", meta=meta)
    
    meta.update(list_to_dict(cols))
    return meta

# Process all dataframe processing functions.
def process_df(df):
    meta = getMeta(df)
    meta = processIpDataframe(df, meta)
    meta = processSplitIpDataframe(df, meta)
    meta = processUserAgentDataframe(df, meta)
    # Start computing.
    df.compute()

### MAIN SCRIPT ###
@click.command()
@click.option("--input", "--i", required=True, help="The input CSV file")
@click.option("--output", "--o", default="ua-blaster-out-"+time.strftime("%Y-%m-%d-%H:%M")+".csv", help="The output CSV file")
def main(input, output):
    # Define file input/output
    IN_FILENAME = input
    OUT_FILENAME = output

    # Create progress bar
    pbar.register()

    # Get CSV file
    df_csv = dd.read_csv(IN_FILENAME)
    process_df(df_csv)
    df_csv.to_csv(OUT_FILENAME, index=False)
    
    geoip.close()

# Run script
if __name__ == "__main__":
    main()