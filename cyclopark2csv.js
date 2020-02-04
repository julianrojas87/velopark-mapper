const fs = require('fs');
const csv = require('@fast-csv/format');

var parkings = JSON.parse(fs.readFileSync('data/cyclopark.json', 'utf8'))['parkings'];
const stream = csv.format({ headers: true});
stream.pipe(fs.createWriteStream('data/cyclopark.csv', 'utf8'));

for(let i in parkings) {
    let p = parkings[i];
    stream.write(p);
}