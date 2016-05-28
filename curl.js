/**
 * Created by valberg on 5/03/2016.
 *
 * Fetches and inserts weather files
 *
 */

var exec = require('child_process').exec;
var mongodb = require('mongodb');
var parser = require('xml2json');

var MongoClient = mongodb.MongoClient;
var mongourl = 'mongodb://localhost:27017/weatherdb';

var documents = ['IDV10460','IDV10465'];

child = exec('curl ftp://ftp.bom.gov.au/anon/gen/fwo/{'+documents.toString()+'}.xml', function(error, stdout){
    if(error !== null) { console.log('exec error: ' + error); }

    var forecasts = stdout.split(/--_curl_--ftp:\/\/ftp.bom.gov.au\/anon\/gen\/fwo\/.+\.xml\n/g);
    forecasts.shift(); // Remove that empty first item

    for(var f = 0; f < forecasts.length; f++) {
        try {
            var data = parser.toJson(forecasts[f], {reversible: false, object: false});
        } catch (err) {
            console.error('Error parsing the file, something went wrong: ' + err);
        }

        var str = data.replace(/\$t/g, "description");
        var obj = JSON.parse(str);
        var area = obj.product.forecast.area;
        for (var i = 0, len = area.length; i < len; i++) {

            area[i]._id = obj.product.amoc.identifier + '-';
            area[i]._id += area[i].aac + '-';

            if (area[i]['forecast-period'] != undefined && area[i]['forecast-period'] instanceof Array) {
                for (var j = 0, leng = area[i]['forecast-period'].length; j < leng; j++) {

                    var entry = JSON.parse(JSON.stringify(area[i]));
                    entry['forecast-period'] = area[i]['forecast-period'][j];

                    formatForecastPeriod(entry);
                    insertWeather(entry);
                }
            } else {
                formatForecastPeriod(area[i]);
                insertWeather(area[i]);
            }
        }
    }
});


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
                entry[key[k].slice(0, key[k].length - 1)] = forecastArray[k];
            }
        }
    }


}

/**
 * Connect to MongoDB and insert the forecast as a JSON object
 * @param d
 */
function insertWeather(d){

    MongoClient.connect(mongourl, function (err, db) {
        if (err) {
            console.log('Unable to connect to the mongoDB server. Error:', err);
            process.exit(1);
        }

        db.collection('forecast').updateOne({"_id": d._id},d,{upsert:true}, function(err, results){

            if(err){
                console.error(new Error("Failed upserting forecast: " + err));
                process.exit(1);
            }
            // console.log('Inserted documents into the "users" collection. The documents inserted with "_id" are:', result.length, result);
            db.close();
            process.exit(0);
        });
    });
}
