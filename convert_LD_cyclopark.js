const fs = require('fs');
const util = require('util');
const request = require('request');
const path = require("path");
const commander = require('commander');

global.program = new commander.Command();
program.version('0.0.1');
program.option('-o, --output-folder <folder>', 'The folder to store the parking files.')
    .option('-l, --live', 'The project is live. This is used to select the proper MongoDB connection string.');
program.parse(process.argv);

const dbAdapter = require('./database-adapter');

const outputFolder = program.outputFolder || 'output/parkings/';
if(!fs.existsSync(outputFolder)){
    console.error('the specified folder does not exist.', outputFolder);
    process.exit(1);
}

const writeFile = util.promisify(fs.writeFile);

async function run() {
    var hrstart = process.hrtime();
    console.log("STARTING");

    // Get content from file
    var contents = fs.readFileSync("data/parkings_201906111038.json");
    // Define to JSON type
    var jsonContent = JSON.parse(contents);
    jsonContent = jsonContent["parkings"];
    let counter = jsonContent.length;
    // Get Value from JSON
    let promises = [];
    let parkings = [];
    let profilePromise = getApplicationProfile();
    profilePromise.catch(error => {
        console.error(error);
    });

    await dbAdapter.initDbAdapter();
    await dbAdapter.insertCompany('Cyclopark', function(){});

    profilePromise.then(jsonLD => {
        for (i in jsonContent) {
            let jsonLDResult = insertValuesInJsonLD(jsonContent[i], jsonLD);

            let fileName = (jsonLDResult['ownedBy']['companyName'] + '_' + jsonLDResult['identifier']).replace(/\s/g, '-') + '.jsonld';
            let location;
            try {
                location = {
                    type: "Point",
                    coordinates: extractLocationFromJsonld(jsonLDResult)
                };
            } catch (e) {
                console.error("Could not extract location from parking." + e);
            }

            writeFile(path.join(outputFolder, fileName), JSON.stringify(jsonLDResult), 'utf8');
            dbAdapter.updateOrCreateParking(encodeURIComponent(jsonLDResult['@id']), fileName, true, location, function(){});
            dbAdapter.updateCompanyParkingIDs('Cyclopark', encodeURIComponent(jsonLDResult['@id']), function(){});
            counter--;
            console.log(i + "\tDone\t(", counter, "left)");
            if (counter <= 0) {
                var hrend = process.hrtime(hrstart);
                console.info("\nFINISHED! took %ds %dms", hrend[0], hrend[1] / 1000000);
                //TODO: close db
            }
        }
    });
}

function extractLocationFromJsonld(jsonld) {
    let geo = jsonld['@graph'][0]["geo"];
    let lonlat = [];
    for (let i = 0; i < geo.length; i++) {
        if (geo[i]["@type"] === "GeoCoordinates") {
            lonlat[0] = geo[i]["longitude"];
            lonlat[1] = geo[i]["latitude"];
        }
    }
    return lonlat;
}

let regex = /(\d{4})/;

function insertValuesInJsonLD(parkingData, applicationProfileString) {
    let jsonLD = JSON.parse(applicationProfileString);
    jsonLD.identifier = parkingData.code;
    jsonLD.name = [{"@value": parkingData.name, "@language": "nl"}];
    jsonLD.ownedBy.companyName = 'Cyclopark';
    jsonLD.operatedBy.companyName = 'Cyclopark';
    let results = parkingData.legacy_address.match(regex);
    jsonLD.address.postalCode = results.length ? results[results.length-1] : '';
    jsonLD.address.streetAddress = parkingData.legacy_address;
    jsonLD.address.country = 'Belgium';
    jsonLD.startDate = parkingData.installation_date;
    jsonLD['@graph'][0]['@type'] = "https://velopark.ilabt.imec.be/openvelopark/terms#PublicBicycleParking";
    jsonLD['@graph'][0]["openingHoursSpecification"] = [
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
    jsonLD['@graph'][0]['maximumParkingDuration'] = formatValue('maximumParkingDuration', 30, jsonLD['@context']);
    jsonLD['@graph'][0]['allows'][0]['bicycleType'] = "https://velopark.ilabt.imec.be/openvelopark/terms#RegularBicycle";
    jsonLD['@graph'][0]['allows'][0]['bicyclesAmount'] = parkingData.capacity_classic;
    if(parkingData.capacity_cargo > 0) {
        jsonLD['@graph'][0]['allows'][1] = {};
        jsonLD['@graph'][0]['allows'][1]['bicycleType'] = "https://velopark.ilabt.imec.be/openvelopark/terms#CargoBicycle";
        jsonLD['@graph'][0]['allows'][1]['bicyclesAmount'] = parkingData.capacity_cargo;
    }
    jsonLD['@graph'][0]['geo'][0]['latitude'] = parkingData.latitude;
    jsonLD['@graph'][0]['geo'][0]['longitude'] = parkingData.longitude;
    jsonLD['@graph'][0]['priceSpecification'][0]['freeOfCharge'] = !parkingData.mobib;

    //Auto fill
    jsonLD['@id'] = 'https://velopark.ilabt.imec.be/data/Cyclopark_' + encodeURIComponent(parkingData.code);
    jsonLD.dateModified = (new Date()).toISOString();
    // Set values for each parking section
    for (let i = 0; i < jsonLD['@graph'].length; i++) {
        // Calculate and set totalCapacity
        let tc = 0;
        for (let j = 0; j < jsonLD['@graph'][i]['allows'].length; j++) {
            tc += jsonLD['@graph'][i]['allows'][j]['bicyclesAmount'] !== '' ? parseInt(jsonLD['@graph'][i]['allows'][j]['bicyclesAmount']) : 0;
        }
        jsonLD['@graph'][i]['totalCapacity'] = tc;

    }
    let lonlat = [jsonLD['@graph'][0]['geo'][0]['longitude'], jsonLD['@graph'][0]['geo'][0]['latitude']];
    jsonLD['hasMap'] = {
        "@type": "Map",
        "url": 'https://www.openstreetmap.org/#map=18/' + lonlat[1] + '/' + lonlat[0]
    };
    cleanEmptyValues(jsonLD);
    return jsonLD;
}

function formatValue(name, value, context) {
    if (context[`${name}`]) {
        let type = context[`${name}`]['@type'];
        if (type) {
            if (type == 'xsd:dateTime' && value) {
                return (new Date(value)).toISOString();
            }
            if (type == 'xsd:integer' && value) {
                try {
                    return parseInt(value);
                } catch (e) {
                    return 0;
                }
            }
            if (type == 'xsd:double' && value) {
                try {
                    return parseFloat(value);
                } catch (e) {
                    return 0.0;
                }
            }
            if (type == 'xsd:boolean' && value !== '') {
                if (value == 'true') {
                    return true;
                } else {
                    return false;
                }
            }
            if (type = 'xsd:duration' && value) {
                if (name == 'maximumParkingDuration') {
                    return 'P' + value + 'D';
                }
                if (name == 'minimumStorageTime') {
                    return 'PT' + value + 'M';
                }
            }
        }
    }
    return value;
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