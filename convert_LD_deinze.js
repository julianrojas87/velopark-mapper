const fs = require('fs');
const util = require('util');
const path = require("path");
const request = require('request');
const csv = require('fast-csv');
const commander = require('commander');
const dbAdapter = require('./database-adapter');
const SphericalMercator = require('@mapbox/sphericalmercator');
const wktParser = require('wellknown');

const readFile = util.promisify(fs.readFile);
const writeFile = util.promisify(fs.writeFile);

program = new commander.Command();
program.version('1.0.0');
program.option('-o, --output-folder <folder>', 'The folder to store the parking files.');
program.parse(process.argv);

const outputFolder = program.outputFolder || 'output/';
if (!fs.existsSync(outputFolder)) {
    console.error('the specified folder does not exist.', outputFolder);
    process.exit(1);
}

const OPEN24_7 = [
    {
        "@type": "OpeningHoursSpecification",
        "dayOfWeek": "http://schema.org/Monday",
        "opens": "00:00",
        "closes": "23:59"
    }, {
        "@type": "OpeningHoursSpecification",
        "dayOfWeek": "http://schema.org/Tuesday",
        "opens": "00:00",
        "closes": "23:59"
    }, {
        "@type": "OpeningHoursSpecification",
        "dayOfWeek": "http://schema.org/Wednesday",
        "opens": "00:00",
        "closes": "23:59"
    }, {
        "@type": "OpeningHoursSpecification",
        "dayOfWeek": "http://schema.org/Thursday",
        "opens": "00:00",
        "closes": "23:59"
    }, {
        "@type": "OpeningHoursSpecification",
        "dayOfWeek": "http://schema.org/Friday",
        "opens": "00:00",
        "closes": "23:59"
    }, {
        "@type": "OpeningHoursSpecification",
        "dayOfWeek": "http://schema.org/Saturday",
        "opens": "00:00",
        "closes": "23:59"
    }, {
        "@type": "OpeningHoursSpecification",
        "dayOfWeek": "http://schema.org/Sunday",
        "opens": "00:00",
        "closes": "23:59"
    }
];

async function run() {
    var hrstart = process.hrtime();
    console.log("STARTING");
    await dbAdapter.initDbAdapter();

    const [appProfile, facilities, services, security, bikes, entrances, shapes] = await Promise.all([
        getApplicationProfile(),
        loadCSV('data/deinze/facility.csv'),
        loadCSV('data/deinze/services.csv'),
        loadCSV('data/deinze/security.csv'),
        loadCSV('data/deinze/bikes.csv'),
        loadGeoJson('data/deinze/entrances.geojson'),
        loadGeoJson('data/deinze/shapes.geojson')
    ]);

    const parkings = fs.createReadStream('data/deinze/parking.csv', 'utf8').pipe(csv.parse({ headers: true }));

    for await (const p of parkings) {
        let jsonLD = insertValuesInJsonLD(
            p,
            facilities.get(p['Locale ID']),
            services.get(p['Locale ID']),
            security.get(p['Locale ID']),
            bikes.get(p['Locale ID']),
            entrances.get(p['Locale ID']),
            shapes.get(p['Locale ID']),
            JSON.parse(appProfile)
        );

        const fileName = `${jsonLD['ownedBy']['companyName'].replace(/\s/g, '-')}_${jsonLD['identifier']}.jsonld`;
        await writeFile(path.join(outputFolder, fileName), JSON.stringify(jsonLD), 'utf8');
        const location = { type: "Point", coordinates: extractLocationFromJsonld(jsonLD) };
        await dbAdapter.updateOrCreateParking(encodeURIComponent(jsonLD['@id']), fileName, true, location);
    }

    var hrend = process.hrtime(hrstart);
    console.info("\nFINISHED! took %ds %dms", hrend[0], hrend[1] / 1000000);
    process.exit();
}

function extractLocationFromJsonld(jsonld) {
    let geo = jsonld['@graph'][0]["geo"][0];
    return [parseFloat(geo['longitude']), parseFloat(geo['latitude'])];
}

function insertValuesInJsonLD(parkingData, fac, ser, sec, bik, ent, shp, jsonLD) {
    const owner = parkingData['E_Naam Organisatie'].replace(/\s/g, '-');
    jsonLD['@id'] = `https://velopark.ilabt.imec.be/data/${owner}_${parkingData['Locale ID']}`;
    jsonLD.dateModified = (new Date()).toISOString();
    jsonLD.identifier = parkingData['Locale ID'];
    jsonLD.name = [{ "@value": parkingData.NAAM, "@language": "nl" }];
    jsonLD.startDate = new Date(2019, 0, 1).toISOString();
    jsonLD.description = [{ "@value": parkingData['Beschrijving'], "@language": "nl" }];
    jsonLD.temporarilyClosed = false;
    jsonLD.ownedBy.companyName = parkingData['E_Naam Organisatie'];
    if (parkingData['E_Website Organisatie'] !== '') {
        jsonLD.ownedBy['@id'] = `https://${parkingData['E_Website Organisatie']}`;
    }
    jsonLD.operatedBy.companyName = parkingData['P_Naam Organisatie'];
    if (parkingData['P_Website Organisatie'] !== '') {
        jsonLD.operatedBy['@id'] = `https://${parkingData['P_Website Organisatie']}`;
    }
    jsonLD.address.postalCode = parkingData['Postcode'];
    jsonLD.address.streetAddress = parkingData['Straat Naam en Nummer'];
    jsonLD.address.country = 'Belgium';
    jsonLD['hasMap']['url'] = `https://www.openstreetmap.org/#map=18/${ent[0]['geometry']['coordinates'][1]}/${ent[0]['geometry']['coordinates'][0]}`;
    jsonLD['contactPoint']['email'] = parkingData['Email'];
    jsonLD['contactPoint']['telephone'] = parkingData['Telefoonnummer'];
    if (parkingData['Website'] !== '') {
        jsonLD['interactionService']['url'] = `https://${parkingData['Website']}`;
    }

    // Process sections
    jsonLD['@graph'] = [];
    let i = 1;
    for (const f of fac) {
        let section = {};
        section['@id'] = `https://velopark.ilabt.imec.be/data/${owner}_${parkingData['Locale ID']}#section${i}`;
        section['@type'] = getSectionType(f['Type Parking']);
        section['sectionName'] = f['Sectie Naam'];
        section['covered'] = f['Overdekt'] === 'JA';
        section['numberOfLevels'] = parseInt(f['Aantal verdiepingen']);
        section['publicAccess'] = f['Publiek toegankelijk'] === 'Yes';
        section['maximumParkingDuration'] = f['Maximale parkeertijd (dagen)'] !== '' ? `P${f['Maximale parkeertijd (dagen)']}D` : 'P30D';
        section['intendedAudience'] = [{ "@value": f['Doelpubliek'], "@language": "nl" }];
        section["openingHoursSpecification"] = OPEN24_7;

        // General services
        const services = getEntityByName(f['Sectie Naam'], ser);
        if (services && services.length > 0) {
            section['amenityFeature'] = [];
            for (const s of services) {
                section['amenityFeature'].push({
                    "@type": getAmenityType(s['Voorziening']),
                    "description": [{ "@value": s['Omschrijving'], "@language": "nl" }],
                    "hoursAvailable": OPEN24_7
                });
            }
        }

        // Security services
        const security = getEntityByName(f['Sectie Naam'], sec);
        if (security && security.length > 0) {
            if (!section['amenityFeature']) section['amenityFeature'] = [];
            for (const s of security) {
                section['amenityFeature'].push({
                    "@type": getAmenityType(s['Voorziening']),
                    "description": [{ "@value": s['Omschrijving'], "@language": "nl" }],
                    "hoursAvailable": OPEN24_7
                });
            }
        }

        // Allowed bikes
        let total = 0;
        const bikes = getEntityByName(f['Sectie Naam'], bik);
        if (bikes && bikes.length > 0) {
            section['allows'] = [];
            for (const b of bikes) {
                total += parseInt(b['Capaciteit']);
                section['allows'].push({
                    "@type": "AllowedBicycle",
                    "bicycleType": getBikeType(b['Type Fiets']),
                    "bicyclesAmount": parseInt(b['Capaciteit']),
                    "countingSystem": false
                });
            }
        }
        section['totalCapacity'] = total;

        // Process main entrance
        const entrances = getEntityByName(f['Sectie Naam'], ent);
        section['geo'] = [];
        for (const e of entrances) {
            section['geo'].push({
                "@type": "GeoCoordinates",
                "latitude": parseFloat(e.geometry.coordinates[1]),
                "longitude": parseFloat(e.geometry.coordinates[0])

            });
        }

        // Process shape
        const shapes = getEntityByName(f['Sectie Naam'], shp);
        for (const s of shapes) {
            section['geo'].push({
                "@type": "GeoShape",
                "polygon": wktParser.stringify(s),
            });
        }

        section['priceSpecification'] = [{
            "@type": "PriceSpecification",
            "freeOfCharge": true
        }];

        jsonLD['@graph'].push(section);
        i++;
    }

    cleanEmptyValues(jsonLD);
    return jsonLD;
}

function getEntityByName(name, arr) {
    if (arr) {
        let res = [];
        for (const el of arr) {
            if (el.type === 'Feature') {
                if (el.properties['Sectie_Nm'] === name) {
                    res.push(el);
                }
            } else {
                if (el['Sectie Naam'] === name) {
                    res.push(el);
                }
            }
        }
        return res;
    }
}

function getSectionType(raw) {
    switch (raw) {
        case 'Openbare Fietsenstalling':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#PublicBicycleParking';
        case 'Buurtstalling':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#ResidentBicycleParking';
        case 'Fietskluis':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#BicycleLocker';
        case 'Fietsbeugel':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#BicycleStand';
    }
}

function getAmenityType(raw) {
    switch (raw) {
        case 'Fietspomp':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#BicyclePump';
        case 'Deelfiets':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#BikeSharing';
        case 'Camerabewaking':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#CameraSurveillance';
    }
}

function getBikeType(raw) {
    switch (raw) {
        case 'Normale Fiets':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#RegularBicycle';
        case 'Elektrische fiets':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#ElectricBicycle';
        case 'Bakfiets':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#CargoBicycle';
        case 'Tandemfiets':
            return 'https://velopark.ilabt.imec.be/openvelopark/terms#TandemBicycle';
    }
}

async function loadGeoJson(path) {
    const merc = new SphericalMercator();
    let map = new Map();
    const gj = JSON.parse(await readFile(path, 'utf8'));
    for (const f of gj.features) {
        /*if (f.geometry.type === 'Polygon') {
            let cs = [];
            for (c of f.geometry.coordinates[0]) {
                cs.push(merc.inverse(c));
            }
            f.geometry.coordinates = [cs];
        } else {
            f.geometry.coordinates = merc.inverse(f.geometry.coordinates);
        }*/

        if (map.has(f.properties['Locale_ID'].toString())) {
            map.get(f.properties['Locale_ID'].toString()).push(f);
        } else {
            map.set(f.properties['Locale_ID'].toString(), [f]);
        }
    }

    return map;
}

async function loadCSV(path) {
    let map = new Map();
    const stream = fs.createReadStream(path, 'utf8').pipe(csv.parse({ headers: true }));

    for await (const d of stream) {
        if (map.has(d['Locale ID'])) {
            map.get(d['Locale ID']).push(d);
        } else {
            map.set(d['Locale ID'], [d]);
        }
    }

    return map;
}

function cleanEmptyValues(obj) {
    let keys = Object.keys(obj);
    for (let i in keys) {
        if (Array.isArray(obj[`${keys[i]}`])) {
            for (let j = obj[`${keys[i]}`].length - 1; j >= 0; j--) {
                cleanEmptyValues(obj[`${keys[i]}`][j]);
                let l = Object.keys(obj[`${keys[i]}`][j]);
                if (l.length == 0 || (l.length == 1 && l[0] == '@type' && keys[i] != 'amenityFeature')) {
                    obj[`${keys[i]}`].splice(j, 1);
                }
            }
            if (obj[`${keys[i]}`].length == 1) {
                let l = Object.keys(obj[`${keys[i]}`][0]);
                if (l.length == 0 || (l.length == 1 && l[0] == '@type' && keys[i] != 'amenityFeature')) {
                    delete obj[`${keys[i]}`];
                }
            } else if (obj[`${keys[i]}`].length == 0) {
                delete obj[`${keys[i]}`];
            }
        } else if (typeof obj[`${keys[i]}`] == 'object') {
            cleanEmptyValues(obj[`${keys[i]}`]);
            let k = Object.keys(obj[`${keys[i]}`]);
            if (k.length == 0 || (k.length == 1 && k[0] == '@type')) {
                delete obj[`${keys[i]}`];
            }
        } else {
            if (obj[`${keys[i]}`] === '') {
                delete obj[`${keys[i]}`];
            }
        }
    }
}

function getApplicationProfile() {
    return new Promise((resolve, reject) => {
        request({
            url: 'http://velopark.ilabt.imec.be/openvelopark/application-profile',
            rejectUnauthorized: false
        }, (error, response, body) => {
            if (error) {
                reject(error);
            } else {
                resolve(body);
            }
        });
    });
}

run();