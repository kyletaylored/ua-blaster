# User Agent Blaster
A short set of scripts to process request logs in CSV format to break down devices and GeoIP data.

## Quick Rundown

1. Install a virtual environment

```bash
virtualenv venv
```

2. Install requirements

```
pip install -r requirements.txt 
```

3. Manually download / setup device_detector

```
git clone https://github.com/thinkwelltwd/device_detector
cd device_detector
easy_install .
```

4. Install Maxmind GeoIP Lite database
Sign up / sign into Maxmind, download GeoIP 2 City database to `GeoLite2/GeoLite2-City.mmdb`

5. Configure input / output files (@todo). Run python script.

```
python blaster.py --input 02-20-2020-output.csv
```

### Credits
<small>This product includes GeoLite2 data created by MaxMind, available from <a href="https://www.maxmind.com">https://www.maxmind.com</a>.</small>
