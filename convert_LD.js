const fs = require('fs');
const util = require('util');
const request = require('request');

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
    for(i in jsonContent){
    //let i = 0;
    //{
        promises[i] = [];
        parkings[i] = {};

        promises[i].push(getAddressHERE(i, jsonContent[i]['latitude'], jsonContent[i]['longitude']));
        //promises[i].push(getPostalCode(i, jsonContent[i]['tblalgemenegegevens.Naam']));

        parkings[i].name = jsonContent[i]['tblalgemenegegevens.Naam'] + " " + jsonContent[i]['tblintermodaliteitfietsenstalling.Naam'];
        parkings[i].localIdentifier = jsonContent[i]['ID fietsenstalling'];
        parkings[i].initialOpening = (new Date("01/01/2000")).toISOString();
        parkings[i].country = "Belgium";
        parkings[i].organizationName1 = (jsonContent[i]['Eigenaar terrein'] === "Derde" || !jsonContent[i]['Eigenaar terrein']) ? jsonContent[i]['Eigenaar derde'] : jsonContent[i]['Eigenaar terrein'];
        parkings[i].organizationName2 = jsonContent[i]['Exploitant'];
        parkings[i].maximumParkingDuration = 30;
        parkings[i].openingTime = "00:00";
        parkings[i].closingTime = "23:59";
        parkings[i].latitude = jsonContent[i]['latitude'];
        parkings[i].longitude = jsonContent[i]['longitude'];
        parkings[i].capacity = jsonContent[i]['Plaatsen totaal fiets'];

        //Promise.all(promises[i]).then((values) => {
        promises[i][0].then(value => {
            let id = value.id;
            parkings[id].address = value.address;
            parkings[id].postalCode = value.postalCode;
            JSON.stringify(parkings[id]);
            writeFile('output/parkings/' + parkings[id].localIdentifier + '.jsonld', JSON.stringify(parkings[id]), 'utf8');
            counter--;
            console.log(id + "\tDone\t(", counter, "left)");
            if(counter <= 0){
                var hrend = process.hrtime(hrstart);
                console.info("\nFINISHED! took %ds %dms",  hrend[0], hrend[1] / 1000000);
            }
        }).catch((error) => {
            console.error(error);
            counter--;
            if(counter <= 0){
                var hrend = process.hrtime(hrstart);
                console.info("\nFINISHED! took %ds %dms",  hrend[0], hrend[1] / 1000000);
            }
        });

    }
    console.log("SHOTS FIRED\n");
}

function getAddressHERE(i, lat, lon, attempts = 3){
    // AppId: nAv28dOa7NYpJwhkOM25
    // AppCode: ZydaNWn7tOQXq77y6FUKqA
    let requestOptions = {
        url : 'https://reverse.geocoder.api.here.com/6.2/reversegeocode.json?prox=' + lat +'%2C' + lon + '%2C50&mode=retrieveAddresses&maxresults=1&gen=9&app_id=nAv28dOa7NYpJwhkOM25&app_code=ZydaNWn7tOQXq77y6FUKqA',
        headers: {
            'User-Agent' : 'request'
        }
    };

    return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
            if(error){
                console.warn(i, "Request failed, attempts left: ", attempts);
                if(attempts > 0){
                    setTimeout(function () {
                        getAddressHERE(i, lat, lon, attempts-1).then(res => { resolve(res)}).catch(e => {reject(e)});
                    }, 300 + Math.random()*400);
                } else {
                    console.error(i, "getAddress Failed for id " + i, error, response);
                    reject(error);
                }
            } else {
                try {
                    //console.log(body);
                    let bodyObj = JSON.parse(body);
                    resolve({id: i, address: bodyObj.Response.View[0].Result[0].Location.Address.Label, postalCode: bodyObj.Response.View[0].Result[0].Location.Address.PostalCode});
                } catch (e) {
                    console.log(i, "Request failed, attempts left: ", attempts);
                    if(attempts > 0){
                        setTimeout(function () {
                            getAddressHERE(i, lat, lon, attempts-1).then(res => { resolve(res)}).catch(e => {reject(e)});
                        }, 300 + Math.random()*400);
                    } else {
                        reject(e);
                    }
                }
            }
        });
    });
}

function getAddressGoogle(i, lat, lon, attempts = 3){
    let requestOptions = {
        url : 'https://maps.googleapis.com/maps/api/geocode/json?latlng=' + lat + ',' + lon + '&result_type=street_address&key=AIzaSyCK6rvg1XXk2ebo44SOoRjmGStFV0tJEZU',
        headers: {
            'User-Agent' : 'request'
        }
    };

    return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
            if(error){
                console.warn(i, "Request failed, attempts left: ", attempts);
                if(attempts > 0){
                    setTimeout(function () {
                        getAddressGoogle(i, lat, lon, attempts-1).then(res => { resolve(res)}).catch(e => {reject(e)});
                    }, 300 + Math.random()*400);
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
                    while(!bodyObj.results[index].types.includes("street_address") && index < bodyObj.results.length){
                        index++;
                    }
                    if(index === bodyObj.results.length){
                        console.error(i, "No address found in response from Google");
                    } else {
                        address = bodyObj.results[index].formatted_address;
                    }

                    //find postal code
                    let postalCode;
                    let addressComponents = bodyObj.results[index].address_components;
                    let componentIndex = 0;
                    while(!addressComponents[componentIndex].types.includes("postal_code") && componentIndex < addressComponents.length){
                        console.log(componentIndex, ":", addressComponents[componentIndex]);
                        componentIndex++;
                    }
                    if(componentIndex === addressComponents.length){
                        console.error(i, "No postal code found in response from Google");
                    } else {
                        postalCode = addressComponents[componentIndex].long_name;
                        console.log("postcode:", postalCode);
                    }

                    resolve({id: i, address: address, postalCode: postalCode});
                } catch (e) {
                    console.log(i, "Request failed, attempts left: ", attempts);
                    if(attempts > 0){
                        setTimeout(function () {
                            getAddressGoogle(i, lat, lon, attempts-1).then(res => { resolve(res)}).catch(e => {reject(e)});
                        }, 300 + Math.random()*400);
                    } else {
                        reject(e);
                    }
                }
            }
        });
    });
}

function getAddressNominatim(i, lat, lon, attempts = 10){
    let requestOptions = {
        url : 'https://nominatim.openstreetmap.org/reverse?format=json&lat=' + lat + "&lon=" + lon,
        headers: {
            'User-Agent' : 'request'
        }
    };

    return new Promise((resolve, reject) => {
        request(requestOptions, (error, response, body) => {
            if(error){
                console.warn(i, "Request failed, attempts left: ", attempts);
                if(attempts > 0){
                    setTimeout(function () {
                        getAddressNominatim(i, lat, lon, attempts-1).then(res => { resolve(res)}).catch(e => {reject(e)});
                    }, 300 + Math.random()*400);
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
                    if(attempts > 0){
                        setTimeout(function () {
                            getAddressNominatim(i, lat, lon, attempts-1).then(res => { resolve(res)}).catch(e => {reject(e)});
                        }, 300 + Math.random()*400);
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