/**
 * Created by Valberg on 6/03/2016.
 *
 * Download the XML documents from BOM
 * Only downloads the ones that have been marked for updates
 * and only those who's "next-update" is in the past.
 * Then once the download is complete, run the file attribute updates for mongo.
 */

var mongodb = require('mongodb'),
    assert = require('assert');
var exec = require('child_process').exec;

var MongoClient = mongodb.MongoClient;
var mongourl = 'mongodb://localhost:27017/weatherdb';
var findFilter = {"update":1, "next_update": {$lte: new Date()}};
//var findFilter = {"service":"WSM-FLW"};

MongoClient.connect(mongourl, function (err, db) {
    if (err) { console.log('Unable to connect to the mongoDB server. Error:', err); }

    db.collection('file')
        .find(findFilter)
        .project({bomId:1,_id:0})
        .toArray(function(err, results) {
            assert.equal(err, null);
            var ids = new Array();
            for(var i =0; i < results.length; i++){
                ids[i] = results[i].bomId;
            }
            child = exec('curl -v ftp://ftp.bom.gov.au/anon/gen/fwo/{'+ids.toString()+'}.xml -o "weather-files/#1.xml"');
            child.on('close', function(){
                db.close();
                // Finished downloading, now run the file attributes update to mongoDB
                require('./update-file-attributes');
                require('./upsert-marine');
            })
        });
});

