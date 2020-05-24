import fs from 'fs';
import util from 'util';
import csv from '@fast-csv/parse';
import geolib from 'geolib';

async function run() {

    let parkings = {
        "type": "FeatureCollection",
        "name": "SelectieVelopark",
        "features": []
    };

    /*let orig = JSON.parse(await readFile('data/new_nmbs_epsg4313.json', 'utf8'));
    let newCoords = JSON.parse(await readFile('data/new_nmbs_epsg3857_locations.json'));

    for(let i in newCoords) {
        orig['features'][i]['geometry']['coordinates'] = newCoords[i];
    }

    orig['crs']['properties']['name'] = 'urn:ogc:def:crs:EPSG::3857';
    
    await writeFile('data/new_nmbs_epsg3857.json', JSON.stringify(orig));*/

    fs.createReadStream('data/nmbs/nmbs_v3.csv', { encoding: 'utf8', objectMode: true })
        .pipe(csv.parse({ objectMode: true, headers: true }))
        .on('data', data => {
            let lat = parseFloat(geolib.toDecimal(data['Y_WGS'].replace(/,/g, '.')));
            let lon = parseFloat(geolib.toDecimal(data['X_WGS'].replace(/,/g, '.')));
            
            parkings.features.push({
                type: "Feature",
                properties: data,
                geometry: {
                    type: "Point",
                    coordinates: [lon, lat]
                }
            });
        }).on('finish', () => {
            fs.writeFileSync('data/nmbs/nmbs_v3_epsg4313.geojson', JSON.stringify(parkings), 'utf8');
        });
}


run();
