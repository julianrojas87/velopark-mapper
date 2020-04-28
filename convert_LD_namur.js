const fs = require('fs');
const util = require('util');
const request = require('request');
const path = require("path");
const csv = require('fast-csv');
const commander = require('commander');
const dbAdapter = require('./database-adapter');

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

async function run() {
    var hrstart = process.hrtime();
    console.log("STARTING");
    await dbAdapter.initDbAdapter();
    const appProfile = await getApplicationProfile();

    const parkings = JSON.parse(await readFile('data/namur-mobilitie/namur-velo.json', 'utf8'));

    let i = 0;
    for (const parking of parkings) {
        console.log(`processing parking ${i}`);
        let jsonLDResult = await insertValuesInJsonLD(parking, JSON.parse(appProfile));
        let fileName = `${jsonLDResult['ownedBy']['companyName'].replace(/\s/g, '-')}_${jsonLDResult['identifier']}.jsonld`;
        await writeFile(path.join(outputFolder, fileName), JSON.stringify(jsonLDResult), 'utf8');
        let location = { type: "Point", coordinates: extractLocationFromJsonld(jsonLDResult) };
        await dbAdapter.updateOrCreateParking(encodeURIComponent(jsonLDResult['@id']), fileName, true, location);
        i++;
    }

    var hrend = process.hrtime(hrstart);
    console.info("\nFINISHED! took %ds %dms", hrend[0], hrend[1] / 1000000);
    process.exit();
}

function extractLocationFromJsonld(jsonld) {
    let geo = jsonld['@graph'][0]["geo"][0];
    return [parseFloat(geo['longitude']), parseFloat(geo['latitude'])];
}

async function insertValuesInJsonLD(parkingData, jsonLD) {
    const code = parkingData['recordid'];
    const name = parkingData.fields['nom_station'];
    const lat = parkingData.geometry.coordinates[1];
    const lon = parkingData.geometry.coordinates[0];

    let address = await getAddressHERE(lat, lon);

    jsonLD['@id'] = `https://velopark.ilabt.imec.be/data/Namur-Mobilité_${code}`;
    jsonLD.dateModified = (new Date()).toISOString();
    jsonLD.identifier = code;
    jsonLD.name = [{ "@value": name, "@language": "fr" }];
    jsonLD.temporarilyClosed = false;
    jsonLD.ownedBy.companyName = 'Namur Mobilité';
    jsonLD.ownedBy['@id'] = 'https://www.namur.be/fr/ma-ville/mobilite/mobilite';
    jsonLD.operatedBy.companyName = 'Namur Mobilité';
    jsonLD.operatedBy['@id'] = 'https://www.namur.be/fr/ma-ville/mobilite/mobilite';
    jsonLD.address.postalCode = address.postalCode;
    jsonLD.address.streetAddress = address.address.split(',')[0];
    jsonLD.address.country = 'Belgium';
    jsonLD['hasMap']['url'] = `https://www.openstreetmap.org/#map=18/${lat}/${lon}`;
    jsonLD['contactPoint']['email'] = 'mobilite@ville.namur.be';
    jsonLD['contactPoint']['telephone'] = '+32 (0) 81 24 60 86';
    jsonLD['interactionService']['url'] = 'https://www.namur.be/fr/ma-ville/mobilite/mobilite/service-mobilite/service-mobilite';

    jsonLD['@graph'][0]['@id'] = `https://velopark.ilabt.imec.be/data/Namur-Mobilité_${code}#section1`;
    jsonLD['@graph'][0]['covered'] = parkingData.fields['couvert'] === 'Oui';
    jsonLD['@graph'][0]['@type'] = 'https://velopark.ilabt.imec.be/openvelopark/terms#BicycleStand';
    jsonLD['@graph'][0]['numberOfLevels'] = 1;
    jsonLD['@graph'][0]['publicAccess'] = true;
    jsonLD['@graph'][0]['maximumParkingDuration'] = 'P30D';
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
    jsonLD['@graph'][0]['totalCapacity'] = parkingData.fields['support_place'] + parkingData.fields['place_place'];
    jsonLD['@graph'][0]['allows'][0]['bicycleType'] = "https://velopark.ilabt.imec.be/openvelopark/terms#RegularBicycle";
    jsonLD['@graph'][0]['allows'][0]['bicyclesAmount'] = parkingData.fields['support_place'] + parkingData.fields['place_place'];
    jsonLD['@graph'][0]['allows'][0]['countingSystem'] = false;
    jsonLD['@graph'][0]['geo'][0]['latitude'] = parseFloat(lat);
    jsonLD['@graph'][0]['geo'][0]['longitude'] = parseFloat(lon);

    if (parkingData.fields['securise'] === 'Oui') {

        if (parkingData.fields['securise_type'] === 'electric') {
            jsonLD['@graph'][0]['amenityFeature'][0] = {
                "@type": 'https://velopark.ilabt.imec.be/openvelopark/terms#ElectronicAccess'
            }
        }

        if(parkingData.fields['securise_type'] === 'camera + gardien') {
            jsonLD['@graph'][0]['amenityFeature'][0] = {
                "@type": 'https://velopark.ilabt.imec.be/openvelopark/terms#PersonnelSupervision'
            }
            jsonLD['@graph'][0]['amenityFeature'][1] = {
                "@type": 'https://velopark.ilabt.imec.be/openvelopark/terms#CameraSurveillance'
            }
        }
    }

    jsonLD['@graph'][0]['priceSpecification'][0]['freeOfCharge'] = true;

    cleanEmptyValues(jsonLD);
    return jsonLD;
}

function getAddressHERE(lat, lon, attempts = 3) {
    // AppId: nAv28dOa7NYpJwhkOM25
    // AppCode: ZydaNWn7tOQXq77y6FUKqA
    let requestOptions = {
        url: 'https://reverse.geocoder.api.here.com/6.2/reversegeocode.json?prox=' + lat + '%2C' + lon + '%2C50&mode=retrieveAddresses&maxresults=1&gen=9&app_id=nAv28dOa7NYpJwhkOM25&app_code=ZydaNWn7tOQXq77y6FUKqA',
        headers: {
            'User-Agent': 'request'
        }
    };

    return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
            if (error) {
                console.warn("Request failed, attempts left: ", attempts);
                if (attempts > 0) {
                    setTimeout(function () {
                        getAddressHERE(lat, lon, attempts - 1).then(res => {
                            resolve(res)
                        }).catch(e => {
                            reject(e)
                        });
                    }, 300 + Math.random() * 400);
                } else {
                    console.error("getAddress Failed", error, response);
                    reject(error);
                }
            } else {
                try {
                    //console.log(body);
                    let bodyObj = JSON.parse(body);
                    resolve({
                        address: bodyObj.Response.View[0].Result[0].Location.Address.Label,
                        postalCode: bodyObj.Response.View[0].Result[0].Location.Address.PostalCode
                    });
                } catch (e) {
                    console.log("Request failed, attempts left: ", attempts);
                    if (attempts > 0) {
                        setTimeout(function () {
                            getAddressHERE(lat, lon, attempts - 1).then(res => {
                                resolve(res)
                            }).catch(e => {
                                reject(e)
                            });
                        }, 300 + Math.random() * 400);
                    } else {
                        reject(e);
                    }
                }
            }
        });
    });
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