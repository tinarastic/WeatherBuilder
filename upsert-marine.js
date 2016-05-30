/**
 * Created by valberg on 14/03/2016.
 */

/**
 * Read all the weather files and upsert all the areas we are able to, and make a note of the ones we cant.
 */
const mongodb = require('mongodb'),
    assert = require('assert'),
    exec = require('child_process').exec,
    parser = require('xml2json'),
    fs = require('fs'),
    async = require('async');

const folder = 'weather-files/',
    collection = 'file';

var MongoClient = mongodb.MongoClient;
const mongourl = 'mongodb://localhost:27017/weatherdb';

MongoClient.connect(mongourl, function(err, db) {
    db.collection(collection).find({service:{$in: ["WSM-FCW","WSM-FLW"]}}).project({bomId:1,_id:0}).toArray(function(err,document){
        for(var i=0; i<document.length; i++){
            fs.readFile(folder+document[i].bomId+'.xml', 'utf8', function(err, contents) {
                try {
                    var data = parser.toJson(contents, {reversible: false, object: false});
                } catch (err) {
                    console.error('Error parsing the file, something went wrong: ' + err);
                }
                var str = data.replace(/\$t/g, "description");
                var obj = JSON.parse(str);

                /** @todo We need to determine the type of document here so we can treat it appropriately */

                var area = obj.product.forecast.area;
                for (var i = 0, len = area.length; i < len; i++) {
                    area[i].identifier = obj.product.amoc.identifier;
                    area[i]._id = area[i].aac + '_';
                    if (area[i]['forecast-period'] != undefined && area[i]['forecast-period'] instanceof Array) {
                        for (var j = 0, leng = area[i]['forecast-period'].length; j < leng; j++) {
                            var entry = JSON.parse(JSON.stringify(area[i]));
                            entry['forecast-period'] = area[i]['forecast-period'][j];
                            formatForecastPeriod(entry);
                            upsertForecast(db,entry);
                        }
                    } else if (area[i]['forecast-period'] != undefined) {
                        formatForecastPeriod(area[i]);
                        upsertForecast(db,area[i]);
                    } else {
                        /** @todo could be a storm warning */
                    }
                }
            });
        }
    });
});

function upsertForecast(db, d) {
    db.collection('marine_forecast').updateOne({"_id": d._id},d,{upsert:true}, function(err, results){
        if(err){
            console.error(new Error("Failed upserting forecast: " + err));
        }
    });
}

function formatForecastPeriod(entry){

    entry._id += entry['forecast-period']['start-time-local'].slice(0, 10);
    if (entry['forecast-period']['text'] != undefined) {
        entry['entry_type'] = entry['forecast-period']['text']['type'];
        entry['forecast'] = entry['forecast-period']['text']['description'];
        entry['date'] = entry['forecast-period']['start-time-local'].slice(0, 10);
        delete entry['forecast-period'];
    }

    if(entry['forecast'] != undefined) {
        var forecastArray = entry['forecast'].split(/[a-zA-Z0-9]+: /);
        if(forecastArray.length > 0) {
            forecastArray.shift(); // Remove that empty first item
            var key = entry['forecast'].match(/Winds:|Seas:|Weather:/g);
            for (var k = 0; k < forecastArray.length; k++) {
                if(typeof key[k] != "undefined" ){
                    entry[key[k].slice(0, key[k].length - 1)] = forecastArray[k];
                }
            }
        }
    }


}
