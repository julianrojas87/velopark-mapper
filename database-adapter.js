const MongoClient = require('mongodb').MongoClient;

process.env.DB_HOST = process.env.DB_HOST || 'localhost'
process.env.DB_PORT = process.env.DB_PORT || 27017;
process.env.DB_NAME = process.env.DB_NAME || 'node-login';
process.env.DB_URL = 'mongodb://' + process.env.DB_HOST + ':' + process.env.DB_PORT;

var db, parkings, companies;

exports.initDbAdapter = function () {
    return new Promise((resolve, reject) => {
        MongoClient.connect(process.env.DB_URL, { useNewUrlParser: true }, function (e, client) {
            if (e) {
                console.error(e);
            } else {
                db = client.db(process.env.DB_NAME);
                parkings = db.collection('parkings');
                companies = db.collection('companies');

                console.log('mongo :: connected to database :: "' + process.env.DB_NAME + '"');

                resolve();
            }
        });
    });
};

exports.insertCompany = function (companyName) {
    return companies.findOneAndUpdate(
        { name: companyName },
        {
            $set: {
                name: companyName,
                parkingIDs: []
            }
        },
        { upsert: true });
};

exports.deleteAllFromCompany = company => {
    return parkings.deleteMany({
        'parkingID': { $regex: ".*" + company + ".*" },
    });
};

exports.updateCompanyParkingIDs = function (companyName, parkingID) {
    return companies.findOneAndUpdate(
        {
            name: companyName
        },
        {
            $addToSet: {
                parkingIDs: parkingID
            }
        },
        {
            returnOriginal: false
        }
    );
};

exports.updateOrCreateParking = function (id, filename, approvedStatus, location) {
    return parkings.findOneAndUpdate(
        {
            parkingID: id
        },
        {
            $set: {
                filename: filename,
                approvedstatus: approvedStatus,
                location: location
            },
        },
        {
            returnOriginal: false,
            upsert: true
        });
};