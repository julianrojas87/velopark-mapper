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
    var contents = fs.readFileSync("output/nmbs_parkings_geolocated.json");
    // Define to JSON type
    var jsonContent = JSON.parse(contents);
    let counter = jsonContent.length;
    // Get Value from JSON
    let promises = [];
    let parkings = [];
    let profilePromise = getApplicationProfile();
    profilePromise.catch(error => {
        console.error(error);
    });

    await dbAdapter.initDbAdapter();
    await dbAdapter.insertCompany('NMBS', function(){});

    profilePromise.then(jsonLD => {
        for (i in jsonContent) {
            promises[i] = [];
            parkings[i] = {};

            promises[i].push(getAddressHERE(i, jsonContent[i]['latitude'], jsonContent[i]['longitude']));
            //promises[i].push(getPostalCode(i, jsonContent[i]['tblalgemenegegevens.Naam']));

            parkings[i].name = jsonContent[i]['tblalgemenegegevens.Naam'] + " " + jsonContent[i]['tblintermodaliteitfietsenstalling.Naam'];
            parkings[i].localIdentifier = jsonContent[i]['ID fietsenstalling'];
            parkings[i].country = "Belgium";
            parkings[i].organizationName1 = (jsonContent[i]['Eigenaar terrein'] === "Derde" || !jsonContent[i]['Eigenaar terrein']) ? jsonContent[i]['Eigenaar derde'] : jsonContent[i]['Eigenaar terrein'];
            parkings[i].organizationName2 = jsonContent[i]['Exploitant'] || parkings[i].organizationName1;
            parkings[i].maximumParkingDuration = 30;
            parkings[i].openingTime = "00:00";
            parkings[i].closingTime = "23:59";
            parkings[i].latitude = parseFloat(jsonContent[i]['latitude']);
            parkings[i].longitude = parseFloat(jsonContent[i]['longitude']);
            parkings[i].free = jsonContent[i]['Betalend?'] !== "1";
            parkings[i].cameraSurveillance = jsonContent[i]['Camerabewaking'] === "1";
            parkings[i].electronicAccess = jsonContent[i]['Toegangscontrole?'] === "1";
            parkings[i].description = jsonContent[i]['Positie'];

            if(parseInt(jsonContent[i]['Plaatsen bakfiets overdekt']) > 0 || parseInt(jsonContent[i]['Plaatsen bakfiets openlucht']) > 0) {
                parkings[i].cargoBikes = parseInt(jsonContent[i]['Plaatsen bakfiets overdekt']) + parseInt(jsonContent[i]['Plaatsen bakfiets openlucht']);
            }
            if(parseInt(jsonContent[i]['Plaatsen elektrische fiets overdekt']) > 0 || parseInt(jsonContent[i]['Plaatsen elektrische fiets openlucht']) > 0) {
                parkings[i].electricBikes = parseInt(jsonContent[i]['Plaatsen elektrische fiets overdekt']) + parseInt(jsonContent[i]['Plaatsen elektrische fiets openlucht']);
            }
            if(parseInt(jsonContent[i]['Plaatsen fietsen overdekt']) > 0 || parseInt(jsonContent[i]['Plaatsen fietsen openlucht']) > 0) {
                parkings[i].regularBikes = parseInt(jsonContent[i]['Plaatsen fietsen overdekt']) + parseInt(jsonContent[i]['Plaatsen fietsen openlucht']);
            }

            //Promise.all(promises[i]).then((values) => {

            promises[i][0].then(value => {
                let id = value.id;
                parkings[id].address = value.address;
                parkings[id].postalCode = value.postalCode;
                //insert parking values in jsonLD
                let jsonLDResult = insertValuesInJsonLD(parkings[id], jsonLD);

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
                dbAdapter.updateCompanyParkingIDs('NMBS', encodeURIComponent(jsonLDResult['@id']), function(){});
                counter--;
                console.log(id + "\tDone\t(", counter, "left)");
                if (counter <= 0) {
                    var hrend = process.hrtime(hrstart);
                    console.info("\nFINISHED! took %ds %dms", hrend[0], hrend[1] / 1000000);
                    //TODO: close db
                }
            }).catch((error) => {
                console.error(error);
                counter--;
                if (counter <= 0) {
                    var hrend = process.hrtime(hrstart);
                    console.info("\nFINISHED! took %ds %dms", hrend[0], hrend[1] / 1000000);
                    //TODO: close db
                }
            });

        }
        console.log("SHOTS FIRED\n");
    });
}

function extractLocationFromJsonld(jsonld) {
    let geo = jsonld['@graph'][0]["geo"];
    let lonlat = [];
    for (let i = 0; i < geo.length; i++) {
        if (geo[i]["@type"] === "GeoCoordinates") {
            lonlat[0] = parseFloat(geo[i]["longitude"]);
            lonlat[1] = parseFloat(geo[i]["latitude"]);
        }
    }
    return lonlat;
}

function insertValuesInJsonLD(parkingData, applicationProfileString) {
    let jsonLD = JSON.parse(applicationProfileString);
    jsonLD.identifier = parkingData.localIdentifier;
    jsonLD.name = [{"@value": parkingData.name, "@language": "nl"}];
    jsonLD.ownedBy.companyName = 'NMBS';//parkingData.organizationName1;
    jsonLD.operatedBy.companyName = parkingData.organizationName2;
    jsonLD.address.postalCode = parkingData.postalCode;
    jsonLD.address.streetAddress = parkingData.address;
    jsonLD.address.country = parkingData.country;
    jsonLD.address.description = [{"@value": parkingData.description, "@language": "nl"}];
    jsonLD.startDate = parkingData.initialOpening;
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
    jsonLD['@graph'][0]['maximumParkingDuration'] = formatValue('maximumParkingDuration', parkingData.maximumParkingDuration, jsonLD['@context']);
    jsonLD['@graph'][0]['geo'][0]['latitude'] = parkingData.latitude;
    jsonLD['@graph'][0]['geo'][0]['longitude'] = parkingData.longitude;
    jsonLD['@graph'][0]['priceSpecification'][0]['freeOfCharge'] = parkingData.free;
    jsonLD['@graph'][0]['allows'] = [];
    if(parkingData.regularBikes) {
        jsonLD['@graph'][0]['allows'].push({ "@type": "AllowedBicycle", "bicycleType": "https://velopark.ilabt.imec.be/openvelopark/terms#RegularBicycle", "bicyclesAmount": parkingData.regularBikes });
    }
    if(parkingData.cargoBikes) {
        jsonLD['@graph'][0]['allows'].push({ "@type": "AllowedBicycle", "bicycleType": "https://velopark.ilabt.imec.be/openvelopark/terms#CargoBicycle", "bicyclesAmount": parkingData.cargoBikes });
    }
    if(parkingData.electricBikes) {
        jsonLD['@graph'][0]['allows'].push({ "@type": "AllowedBicycle", "bicycleType": "https://velopark.ilabt.imec.be/openvelopark/terms#ElectricBicycle", "bicyclesAmount": parkingData.electricBikes });
    }

    jsonLD['@graph'][0]['amenityFeature'] = [];
    if(parkingData.cameraSurveillance){
        jsonLD['@graph'][0]['amenityFeature'].push({
            "@type": "https://velopark.ilabt.imec.be/openvelopark/terms#CameraSurveillance",
            "hoursAvailable": [
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Monday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Tuesday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Wednesday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Thursday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Friday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Saturday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Sunday",
                    "opens": "00:00",
                    "closes": "23:59"
                }
            ]
        });
    }
    if(parkingData.electronicAccess){
        jsonLD['@graph'][0]['amenityFeature'].push({
            "@type": "https://velopark.ilabt.imec.be/openvelopark/terms#ElectronicAccess",
            "hoursAvailable": [
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Monday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Tuesday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Wednesday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Thursday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Friday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Saturday",
                    "opens": "00:00",
                    "closes": "23:59"
                },
                {
                    "@type": "OpeningHoursSpecification",
                    "dayOfWeek": "http://schema.org/Sunday",
                    "opens": "00:00",
                    "closes": "23:59"
                }
            ]
        });
    }

    //Auto fill
    jsonLD['@id'] = 'https://velopark.ilabt.imec.be/data/NMBS_' + encodeURIComponent(parkingData.localIdentifier);
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

function getAddressHERE(i, lat, lon, attempts = 3) {
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
                console.warn(i, "Request failed, attempts left: ", attempts);
                if (attempts > 0) {
                    setTimeout(function () {
                        getAddressHERE(i, lat, lon, attempts - 1).then(res => {
                            resolve(res)
                        }).catch(e => {
                            reject(e)
                        });
                    }, 300 + Math.random() * 400);
                } else {
                    console.error(i, "getAddress Failed for id " + i, error, response);
                    reject(error);
                }
            } else {
                try {
                    //console.log(body);
                    let bodyObj = JSON.parse(body);
                    resolve({
                        id: i,
                        address: bodyObj.Response.View[0].Result[0].Location.Address.Label,
                        postalCode: bodyObj.Response.View[0].Result[0].Location.Address.PostalCode
                    });
                } catch (e) {
                    console.log(i, "Request failed, attempts left: ", attempts);
                    if (attempts > 0) {
                        setTimeout(function () {
                            getAddressHERE(i, lat, lon, attempts - 1).then(res => {
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

function getAddressGoogle(i, lat, lon, attempts = 3) {
    let requestOptions = {
        url: 'https://maps.googleapis.com/maps/api/geocode/json?latlng=' + lat + ',' + lon + '&result_type=street_address&key=AIzaSyCK6rvg1XXk2ebo44SOoRjmGStFV0tJEZU',
        headers: {
            'User-Agent': 'request'
        }
    };

    return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
            if (error) {
                console.warn(i, "Request failed, attempts left: ", attempts);
                if (attempts > 0) {
                    setTimeout(function () {
                        getAddressGoogle(i, lat, lon, attempts - 1).then(res => {
                            resolve(res)
                        }).catch(e => {
                            reject(e)
                        });
                    }, 300 + Math.random() * 400);
                } else {
                    console.error(i, "getAddress Failed for id " + i, error, response);
                    reject(error);
                }
            } else {
                try {
                    //console.log(body);
                    let bodyObj = JSON.parse(body);
                    console.log(bodyObj);

                    //find address
                    let address;
                    let index = 0;
                    while (!bodyObj.results[index].types.includes("street_address") && index < bodyObj.results.length) {
                        index++;
                    }
                    if (index === bodyObj.results.length) {
                        console.error(i, "No address found in response from Google");
                    } else {
                        address = bodyObj.results[index].formatted_address;
                    }

                    //find postal code
                    let postalCode;
                    let addressComponents = bodyObj.results[index].address_components;
                    let componentIndex = 0;
                    while (!addressComponents[componentIndex].types.includes("postal_code") && componentIndex < addressComponents.length) {
                        console.log(componentIndex, ":", addressComponents[componentIndex]);
                        componentIndex++;
                    }
                    if (componentIndex === addressComponents.length) {
                        console.error(i, "No postal code found in response from Google");
                    } else {
                        postalCode = addressComponents[componentIndex].long_name;
                        console.log("postcode:", postalCode);
                    }

                    resolve({id: i, address: address, postalCode: postalCode});
                } catch (e) {
                    console.log(i, "Request failed, attempts left: ", attempts);
                    if (attempts > 0) {
                        setTimeout(function () {
                            getAddressGoogle(i, lat, lon, attempts - 1).then(res => {
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

function getAddressNominatim(i, lat, lon, attempts = 10) {
    let requestOptions = {
        url: 'https://nominatim.openstreetmap.org/reverse?format=json&lat=' + lat + "&lon=" + lon,
        headers: {
            'User-Agent': 'request'
        }
    };

    return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
            if (error) {
                console.warn(i, "Request failed, attempts left: ", attempts);
                if (attempts > 0) {
                    setTimeout(function () {
                        getAddressNominatim(i, lat, lon, attempts - 1).then(res => {
                            resolve(res)
                        }).catch(e => {
                            reject(e)
                        });
                    }, 300 + Math.random() * 400);
                } else {
                    console.error(i, "getAddress Failed for id " + i, error, response);
                    reject(error);
                }
            } else {
                try {
                    //console.log(body);
                    let bodyObj = JSON.parse(body);
                    resolve({id: i, address: bodyObj.display_name, postalCode: bodyObj.address.postcode});
                } catch (e) {
                    console.log(i, "Request failed, attempts left: ", attempts);
                    if (attempts > 0) {
                        setTimeout(function () {
                            getAddressNominatim(i, lat, lon, attempts - 1).then(res => {
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

/*function getPostalCode(i, city){
    let requestOptions = {
        url : 'http://opzoeken-postcode.be/' + city + '.json',
        headers: {
            'User-Agent' : 'request'
        }
    };

    return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
            if(error){
                reject(error);
            } else {
                //console.log(city, body);
                let postcodes = JSON.parse(body);
                if (postcodes.length) {
                    //console.log(JSON.parse(body)[0]['Postcode']['postcode_deelgemeente']);
                    resolve({id : i, postalCode: JSON.parse(body)[0]['Postcode']['postcode_deelgemeente']});
                } else {
                    resolve({id : i, postalCode: null});
                }
            }
        });
    });
}*/

run();