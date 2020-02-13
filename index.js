const csv = require('csv-parser');
const csvWriter = require('csv-write-stream');
const fs = require('fs');
const flatten = require('flat');
const DeviceDetector = require('device-detector-js');
const deviceDetector = new DeviceDetector();
const cliProgress = require('cli-progress');
const geoip = require('geoip-lite');
const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);

// init CLI bar
bar.start(7500000, 0); 

var writer = csvWriter()
writer.pipe(fs.createWriteStream('bot-out.csv', {flags: 'a'}))

fs.createReadStream('bq-final.csv')
  .pipe(csv())
  .on('data', data => {
    let device = deviceDetector.parse(data.request_user_agent);
    if (device.client !== null) {
      let geodata = geoip.lookup(data.ip);
      let geo = {
        city: "",
        metro: "",
        country: "",
        latitude: "",
        longitude: ""
      }
      if (geodata !== null) {
        geo = {
          city: geodata.city,
          metro: geodata.metro,
          country: geodata.country,
          latitude: geodata.ll[0],
          longitude: geodata.ll[1]
        }
      }
      let res = flatten(device);
      let record = Object.assign({}, data, res, geo);
      writer.write(record);
    }
    bar.increment();
  })
  .on('end', () => {
    writer.end();
    bar.stop();
  });
