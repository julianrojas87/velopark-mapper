const fs = require('fs');
const util = require('util');
//const request = require('request');

const writeFile = util.promisify(fs.writeFile);

async function run() {
    console.log("\n *STARTING* \n");
    // Get content from file
    var contents = fs.readFileSync("output/nmbs_parkings_geolocated.json");
    // Define to JSON type
    var jsonContent = JSON.parse(contents);
    // Get Value from JSON
    for(i in jsonContent){
        console.log(jsonContent[i]['ID fietsenstalling']);
        let parking = {};
        parking.name = jsonContent[i]['tblalgemenegegevens.Naam'] + " " + jsonContent[i]['tblintermodaliteitfietsenstalling.Naam'];
        parking.localIdentifier = jsonContent[i]['ID fietsenstalling'];
        parking.initialOpening = (new Date("01/01/2000")).toISOString();

        JSON.stringify(parking);
        await writeFile('output/parkings/' + parking.localIdentifier + '.jsonld', JSON.stringify(parking), 'utf8');
    }
    console.log("\n *EXIT* \n");
}

run();