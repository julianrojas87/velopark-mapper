const fs = require('fs');
const util = require('util');
const request = require('request');
const path = require("path");
const csv = require('fast-csv');
const commander = require('commander');
const dbAdapter = require('./database-adapter');

const writeFile = util.promisify(fs.writeFile);
const postalCodeRegex = /(\d{4})/;
const priceRegex = /(\d{2,})/;

program = new commander.Command();
program.version('1.0.0');
program.option('-o, --output-folder <folder>', 'The folder to store the parking files.');
program.parse(process.argv);

const outputFolder = program.outputFolder || 'output/';
if (!fs.existsSync(outputFolder)) {
    console.error('the specified folder does not exist.', outputFolder);
    process.exit(1);
}

async function run() {
    var hrstart = process.hrtime();
    console.log("STARTING");
    await dbAdapter.initDbAdapter();
    await dbAdapter.insertCompany('Cyclopark');
    const appProfile = await getApplicationProfile();

    const csvStream = fs.createReadStream('data/cyclopark_new.csv').pipe(csv.parse({ headers: true }))

    for await (const parking of csvStream) {
        let jsonLDResult = insertValuesInJsonLD(parking, JSON.parse(appProfile));
        let fileName = (jsonLDResult['ownedBy']['companyName'] + '_' + jsonLDResult['identifier']).replace(/\s/g, '-') + '.jsonld';
        await writeFile(path.join(outputFolder, fileName), JSON.stringify(jsonLDResult), 'utf8');
        let location = { type: "Point", coordinates: extractLocationFromJsonld(jsonLDResult) };
        await dbAdapter.updateOrCreateParking(encodeURIComponent(jsonLDResult['@id']), fileName, true, location);
        await dbAdapter.updateCompanyParkingIDs('Cyclopark', encodeURIComponent(jsonLDResult['@id']), function () { });
    }

    var hrend = process.hrtime(hrstart);
    console.info("\nFINISHED! took %ds %dms", hrend[0], hrend[1] / 1000000);
    process.exit();
}

function extractLocationFromJsonld(jsonld) {
    let geo = jsonld['@graph'][0]["geo"][0];
    return [parseFloat(geo['longitude']), parseFloat(geo['latitude'])];
}

function insertValuesInJsonLD(parkingData, jsonLD) {
    jsonLD['@id'] = 'https://velopark.ilabt.imec.be/data/Cyclopark_' + parkingData.code.replace(/\s/g, '-');
    jsonLD.dateModified = (new Date()).toISOString();
    jsonLD.identifier = parkingData.code;
    jsonLD.name = [{ "@value": parkingData.name, "@language": "nl" }];
    jsonLD.temporarilyClosed = false;
    jsonLD.ownedBy.companyName = 'Cyclopark';
    jsonLD.ownedBy['@id'] = `https://${parkingData.website}`;
    jsonLD.operatedBy.companyName = 'Cyclopark';
    jsonLD.operatedBy['@id'] = `https://${parkingData.website}`;
    let postalCode = priceRegex.exec(parkingData.addressNL);
    jsonLD.address.postalCode = postalCode ? postalCode[0] : '';
    jsonLD.address.streetAddress = parkingData.addressNL;
    jsonLD.address.country = 'Belgium';
    jsonLD['hasMap']['url'] = `https://www.openstreetmap.org/#map=18/${parkingData.latitude}/${parkingData.longitude}`;
    jsonLD['contactPoint']['email'] = parkingData.Mail;
    jsonLD['contactPoint']['telephone'] = parkingData.Tel;
    jsonLD['interactionService']['url'] = `https://cycloparking.brussels/fr/contact/`;
    jsonLD['@graph'][0]['covered'] = true;
    jsonLD['@graph'][0]['@type'] = determineParkingType(parkingData.parkingTypeName);
    jsonLD['@graph'][0]['numberOfLevels'] = 1;
    jsonLD['@graph'][0]['publicAccess'] = !parkingData.parkingTypeName.includes('Privéparking');
    jsonLD['@graph'][0]['maximumParkingDuration'] = formatValue('maximumParkingDuration', 30, jsonLD['@context']);
    jsonLD['@graph'][0]['intendedAudience'] = [{ "@language": "nl", "@value": parkingData['Toegankelijk_voor'] }];
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
    jsonLD['@graph'][0]['totalCapacity'] = parseInt(parkingData['capacityClassic']) + parseInt(parkingData['capacityCargo']);

    if (parseInt(parkingData['capacityClassic']) > 0) {
        jsonLD['@graph'][0]['allows'][0]['bicycleType'] = "https://velopark.ilabt.imec.be/openvelopark/terms#RegularBicycle";
        jsonLD['@graph'][0]['allows'][0]['bicyclesAmount'] = parseInt(parkingData['capacityClassic']);
        jsonLD['@graph'][0]['allows'][0]['countingSystem'] = false;
    }

    if (parseInt(parkingData['capacityCargo']) > 0) {
        if (jsonLD['@graph'][0]['allows'][0]['bicycleType'] !== '') {
            jsonLD['@graph'][0]['allows'].push({
                "@type": "AllowedBicycle",
                "bicycleType": "https://velopark.ilabt.imec.be/openvelopark/terms#CargoBicycle",
                "bicyclesAmount": parseInt(parkingData['capacityCargo']),
                "countingSystem": false
            });
        } else {
            jsonLD['@graph'][0]['allows'][0]['bicycleType'] = "https://velopark.ilabt.imec.be/openvelopark/terms#CargoBicycle";
            jsonLD['@graph'][0]['allows'][0]['bicyclesAmount'] = parseInt(parkingData['capacityCargo']);
            jsonLD['@graph'][0]['allows'][0]['countingSystem'] = false;
        }
    } 

    jsonLD['@graph'][0]['geo'][0]['latitude'] = parseFloat(parkingData.latitude);
    jsonLD['@graph'][0]['geo'][0]['longitude'] = parseFloat(parkingData.longitude);
    jsonLD['@graph'][0]['priceSpecification'][0]['freeOfCharge'] = false;
    let price = priceRegex.exec(parkingData.Prijs);
    jsonLD['@graph'][0]['priceSpecification'][0]['price'] = price ? parseInt(price[0]) : '';
    jsonLD['@graph'][0]['priceSpecification'][0]['currency'] = 'EUR';
    jsonLD['@graph'][0]['priceSpecification'][0]['dueForTime']['timeStartValue'] = 0;
    jsonLD['@graph'][0]['priceSpecification'][0]['dueForTime']['timeEndValue'] = 1;
    jsonLD['@graph'][0]['priceSpecification'][0]['dueForTime']['timeUnit'] = 'year';

    cleanEmptyValues(jsonLD);
    return jsonLD;
}

function determineParkingType(raw) {
    if (raw.includes('Fietsparking') || raw.includes('Park + Ride') || raw.includes('Privéparking')) {
        return 'https://velopark.ilabt.imec.be/openvelopark/terms#PublicBicycleParking';
    } else if (raw.includes('Buurtparking')) {
        return 'https://velopark.ilabt.imec.be/openvelopark/terms#ResidentBicycleParking';
    } else if (raw.includes('Fietsbox')) {
        return 'https://velopark.ilabt.imec.be/openvelopark/terms#BicycleLocker';
    } else {
        throw new Error('Unrecognized parking type: ' + raw);
    }
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