const fs = require('fs');
const util = require('util');
const request = require('request');
const csv = require('fast-csv');
const geolib = require('geolib');

const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);


async function run() {

    let parkings = [];

    let orig = JSON.parse(await readFile('data/new_nmbs_epsg4313.json', 'utf8'));
    let newCoords = JSON.parse(await readFile('data/new_nmbs_epsg3857_locations.json'));

    for(let i in newCoords) {
        orig['features'][i]['geometry']['coordinates'] = newCoords[i];
    }

    orig['crs']['properties']['name'] = 'urn:ogc:def:crs:EPSG::3857';
    
    await writeFile('data/new_nmbs_epsg3857.json', JSON.stringify(orig));

    /*fs.createReadStream('data/new_nmbs_parkings.csv', { encoding: 'utf8', objectMode: true })
        .pipe(csv({ objectMode: true, headers: true }))
        .on('data', data => {
            let raw_lat = data['Y_WGS84'].replace(/,/g, '.');
            let raw_lon = data['X_WGS84'].replace(/,/g, '.');
            data['lat'] = parseFloat(geolib.toDecimal(raw_lat));
            data['lon'] = parseFloat(geolib.toDecimal(raw_lon));
            parkings.push(data);
        }).on('finish', () => {
            fs.writeFileSync('data/new_nmbs_parkings_geolocated.json', JSON.stringify(parkings), 'utf8');
            console.log('Finished!');
        });*/
}


run();
