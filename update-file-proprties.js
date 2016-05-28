/**
 * Created by Valberg on 6/03/2016.
 * This script will go through all the documents in the "weather-files" folder and update the database of files
 */
const mongodb = require('mongodb'),
    assert = require('assert'),
    exec = require('child_process').exec,
    parser = require('xml2json'),
    fs = require('fs'),
    async = require('async');

const folder = 'weather-files/',
    collection = 'file';

/**
 * Read id table from database
 */
var MongoClient = mongodb.MongoClient;
const mongourl = 'mongodb://localhost:27017/weatherdb';

/** Get all the files in the directory */
var files = fs.readdirSync(folder);

function readAsync(file, callback) {
    fs.readFile(folder+file, 'utf8', function(err, contents){
        try {
            var data = parser.toJson( contents, {reversible: false, object: false} );
        } catch (err) {
            console.error('Error parsing the file, something went wrong: ' + err);
        }
        var str = data.replace(/\$t/g, "description");
        var obj = JSON.parse(str);
        var document = {};
        document.filesize = Math.round(contents.length/1024);

        if(typeof obj.product != "undefined") {
            document.bomId = obj.product.amoc.identifier;
            document.status = obj.product.amoc.status;
            document.service = obj.product.amoc.service
                + '-'
                + ( (typeof obj.product.amoc['sub-service'] != "undefined") ? obj.product.amoc['sub-service'] : "" );
            document.type = obj.product.amoc['product-type'];
            document.region = obj.product.amoc.source.region;
            if (typeof obj.product.amoc['next-routine-issue-time-local'] != "undefined") {
                var test = new Date(obj.product.amoc['next-routine-issue-time-utc']);
                var test2 = new Date(test.getTime() + (60*60*1000*11));
                document.next_update = test2.toJSON();
            }
        } else if(typeof obj['weather-observations'] != "undefined"){
            document.bomId = obj['weather-observations'].product.id;
            document.type = 'O';
            document.region = obj['weather-observations'].product.name;
        } else {
            console.log('Unknown file type, structure not recognised');
        }
        updateDocument(document);
    });
}

async.map(files, readAsync);

function updateDocument(document) {
    MongoClient.connect(mongourl, function (err, db) {
        if (err) { console.log('Unable to connect to the mongoDB server. Error:', err); }
        db.collection(collection).updateOne({"bomId": document.bomId}, document, {upsert: true}, function (err, results) {
            if (err) { console.error(new Error("Failed upserting forecast: " + err)); }
            db.close();
        });
    });
}
