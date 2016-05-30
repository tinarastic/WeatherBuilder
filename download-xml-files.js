/**
 * Created by Valberg on 6/03/2016.
 *
 * Download the XML documents from BOM
 * Only downloads the ones that have been marked for updates
 * and only those who's "next-update" is in the past.
 */

var mongodb = require('mongodb'),
    assert = require('assert');
var exec = require('child_process').exec;

/**
 * Read id table from database
 */
var MongoClient = mongodb.MongoClient;
var mongourl = 'mongodb://localhost:27017/weatherdb';


var findDocuments = function(db, callback) {
    var collection = db.collection('file');
    var dte = new Date();
    collection.find({"update":1, "next_update": {$lte: new Date()}}).project({bomId:1,_id:0}).toArray(function(err, results) {
        assert.equal(err, null);
        callback(results);
    });
};

var getFiles = function(db, results, callback){
    var ids = new Array();
    for(var i =0; i < results.length; i++){
        ids[i] = results[i].bomId;
    }
    child = exec('curl ftp://ftp.bom.gov.au/anon/gen/fwo/{'+ids.toString()+'}.xml -o "weather-files/#1.xml"', function () {
            console.log('Finished, the files are ready for use'); });
    console.log('Statement executed, now in the process of downloading '+ids.toString());
    process.exit(0);
}

MongoClient.connect(mongourl, function (err, db) {
    if (err) { console.log('Unable to connect to the mongoDB server. Error:', err); }
    findDocuments(db, function(results){
        getFiles(db, results, function(document,left){
                db.close();
                process.exit(0);
        });
    });
});


