const fs = require('fs');
const util = require('util');
const path = require("path");
const xml = require('xml-js');
const commander = require('commander');
const request = require('request');
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
    console.log("STARTING");
    const hrstart = process.hrtime();
    // Get content from file
    const kml = await readFile('data/kortenberg/kortenberg.kml', 'utf8');
    const json = JSON.parse(xml.xml2json(kml, { compact: true }))['kml']['Document'].Folder;

    // Init DB
    await dbAdapter.initDbAdapter();

    // Get application profile
    const appProfile = JSON.parse(await getApplicationProfile());

    for (const p of json.Placemark) {
        await map(p, appProfile);
    }

    const hrend = process.hrtime(hrstart);
    console.info("\nFINISHED! took %ds %dms", hrend[0], hrend[1] / 1000000);
    process.exit();
}

async function map(p, jsonLD) {
    let name = p.name._text;
    let capacity = null;
    if (name.startsWith('Sint')) {
        capacity = name.split('-')[2];
        name = `${name.split('-')[0].trim()}-${name.split('-')[1].trim()}`;
    } else {
        capacity = name.split('-')[1];
        name = name.split('-')[0].trim();
    }

    name = name.replace(/,/g, '').replace(/\s/g, '-');
    if (capacity) {
        if (capacity.includes('+')) {
            capacity = capacity.split('+')[0].trim();
        }
        capacity = parseInt(capacity.trim());
    } else {
        capacity = parseInt(p.description._text);
    }

    const lat = p.Point.coordinates._text.trim().split(',')[1];
    const lon = p.Point.coordinates._text.trim().split(',')[0];
    const address = await getAddressHERE(lat, lon);

    jsonLD['@id'] = 'https://velopark.ilabt.imec.be/data/Gemeente-Kortenberg_' + name;
    jsonLD.dateModified = (new Date()).toISOString();
    jsonLD.identifier = name;
    jsonLD.name = [{ "@value": name, "@language": "nl" }];
    jsonLD.temporarilyClosed = false;
    jsonLD.ownedBy.companyName = 'Gemeente-Kortenberg';
    jsonLD.ownedBy['@id'] = 'https://www.kortenberg.be/';
    jsonLD.operatedBy.companyName = 'Gemeente-Kortenberg';
    jsonLD.operatedBy['@id'] = 'https://www.kortenberg.be/';
    jsonLD.address.postalCode = address.postalCode;
    jsonLD.address.streetAddress = address.address.split(',')[0];
    jsonLD.address.country = 'Belgium';
    jsonLD['hasMap']['url'] = `https://www.openstreetmap.org/#map=18/${lat}/${lon}`;
    jsonLD['contactPoint']['email'] = 'mobility@kortenberg.be';
    jsonLD['contactPoint']['telephone'] = '027553070';
    jsonLD['@graph'][0]['@type'] = "https://velopark.ilabt.imec.be/openvelopark/terms#PublicBicycleParking";
    jsonLD['@graph'][0]['publicAccess'] = true;
    jsonLD['@graph'][0]['numberOfLevels'] = 1;
    jsonLD['@graph'][0]['maximumParkingDuration'] = 'P30D';
    jsonLD['@graph'][0]['covered'] = false;
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
    jsonLD['@graph'][0]['totalCapacity'] = capacity;
    jsonLD['@graph'][0]['allows'][0]['bicycleType'] = "https://velopark.ilabt.imec.be/openvelopark/terms#RegularBicycle";
    jsonLD['@graph'][0]['allows'][0]['bicyclesAmount'] = capacity;
    jsonLD['@graph'][0]['allows'][0]['countingSystem'] = false;
    jsonLD['@graph'][0]['geo'][0]['latitude'] = parseFloat(lat);
    jsonLD['@graph'][0]['geo'][0]['longitude'] = parseFloat(lon);
    jsonLD['@graph'][0]['priceSpecification'][0]['freeOfCharge'] = true;

    cleanEmptyValues(jsonLD);

    const fileName = `Gemeente-Kortenberg_${name}.jsonld`;
    await writeFile(path.join(outputFolder, fileName), JSON.stringify(jsonLD), 'utf8');
    let location = { type: "Point", coordinates: extractLocationFromJsonld(jsonLD) };
    await dbAdapter.updateOrCreateParking(encodeURIComponent(jsonLD['@id']), fileName, true, location);
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

function extractLocationFromJsonld(jsonld) {
    let geo = jsonld['@graph'][0]["geo"][0];
    return [parseFloat(geo['longitude']), parseFloat(geo['latitude'])];
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

run();