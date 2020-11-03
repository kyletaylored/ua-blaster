#!/usr/bin/python3

# Log processing support
import click
import geoip2.database
import time
from device_detector import DeviceDetector
from netaddr import IPAddress
import dask.dataframe as dd

# Multiprocessing support
import multiprocessing
import dask

# Debug support
from pprint import pprint

# This creates a Reader object. You should use the same object
# across multiple requests as creation of it is expensive.
geoip = geoip2.database.Reader('./GeoLite2/GeoLite2-City.mmdb')


# Extract GeoIP data
def get_ip_data(ip: str):
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
        data = [response.city.name, response.country.iso_code, response.location.latitude, response.location.longitude]
    except:
        # data = {"city": "","country": "","latitude": "","longitude": ""}
        data = ["", "", "", ""]

    return data


# Get user agent data
def get_device_data(ua):
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
def split_ip(ip: str):
    try:
        ipc = IPAddress(ip)
        if ipc.version == 6:
            arr = ip.split(':')
            txt = arr[0] + ":" + arr[1] + ":0000:0000:0000:0000:0000:0000"
        else:
            arr = ip.split('.')
            txt = arr[0] + "." + arr[1] + ".0.0"
    except:
        txt = "x.x.x.x"
    return txt


# Get dataframe metadata
def get_metadata(df):
    meta = list(df.head(1).to_dict().keys())
    return list_to_dict(meta)


def list_to_dict(the_list):
    return dict(map(lambda x: (x, 'object'), the_list))


def master_data_processing(ip, ua):
    return get_ip_data(ip) + [split_ip(ip)] + get_device_data(ua)


# Process all dataframe processing functions.
def process_data(df):
    meta = get_metadata(df)
    cols = ["city", "country", "latitude", "longitude", "split_ip", "device_brand", "device_model",
                   "device_type", "os_name", "os_version", "client_name", "client_type", "client_version"]
    # Start computing.
    df[cols] = df.apply(lambda x: master_data_processing(x.ip, x.request_user_agent), axis=1, result_type="expand",
                        meta=meta).compute()

def parallelize_dataframe(df, func):
    n_cores = multiprocessing.cpu_count()
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

### MAIN SCRIPT ###
@click.command()
@click.option("--input", "--i", required=True, help="The input CSV file")
@click.option("--output", "--o", default="ua-blaster-out-" + time.strftime("%Y-%m-%d-%H:%M") + ".csv",
              help="The output CSV file")
def main(input, output):
    # Define file input/output
    IN_FILENAME = input
    OUT_FILENAME = output

    # Get CSV file
    df_csv = dd.read_csv(IN_FILENAME)
    parallelize_dataframe(df_csv, process_data)
    df_csv.to_csv(OUT_FILENAME, index=False)

    geoip.close()


# Run script
if __name__ == "__main__":
    main()
