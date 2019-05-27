const fs = require('fs');
const request = require('request');
const csv = require('fast-csv');


async function run() {
    let [stops_nl, stops_standard_nl] = await getStopMaps('nl');
    let [stops_fr, stops_standard_fr] = await getStopMaps('fr');

    let stops = [];

    fs.createReadStream('data/nmbs_parkings.csv', { encoding: 'utf8', objectMode: true })
        .pipe(csv({ objectMode: true, headers: true }))
        .on('data', data => {
            let station = data['tblalgemenegegevens.Naam'].trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, "");
            if (stops_nl.has(station)) {
                stops.push(addLocation(stops_nl.get(station), data));
            } else if (stops_standard_nl.has(station)) {
                stops.push(addLocation(stops_standard_nl.get(station), data));
            } else if (stops_fr.has(station)) {
                stops.push(addLocation(stops_fr.get(station), data));
            } else if (stops_standard_fr.has(station)) {
                stops.push(addLocation(stops_standard_fr.get(station), data));
            } else {
                // Unmapped parking because we could not find its station
                console.log(station);
            }
        }).on('finish', () => {
            fs.writeFileSync('output/nmbs_parkings_geolocated.json', JSON.stringify(stops), 'utf8');
        });
}

function addLocation(loc, station) {
    station['latitude'] = loc['lat'];
    station['longitude'] = loc['lon'];

    return station;
}

function getStopMaps(lang) {
    return new Promise((resolve, reject) => {
        request('https://api.irail.be/stations/?format=json&lang=' + lang, (error, response, body) => {
            let rawResponse = JSON.parse(body)['station'];
            let stopsMap = new Map();
            let stopsStandardMap = new Map();
            
            for (let i in rawResponse) {
                let name = rawResponse[i]['name'].trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, "");
                let standardname = rawResponse[i]['name'].trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, "");
                stopsStandardMap.set(standardname, { 'lat': rawResponse[i]['locationY'], 'lon': rawResponse[i]['locationX'] });
                stopsMap.set(name, { 'lat': rawResponse[i]['locationY'], 'lon': rawResponse[i]['locationX'] });
            }

            resolve([stopsMap, stopsStandardMap]);
        });
    });
}

run();
