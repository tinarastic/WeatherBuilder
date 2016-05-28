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
    db.collection(collection).find({service:{$in: ["WSP-FTW","WSP-","WSP-FDS","WSP-FCT","WSP-FPR","WSP-FLW","WSP-FIS","WSP-FCW","WSP-FAA"]}}).project({bomId:1,_id:0}).toArray(function(err,document){
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

                if(obj.product.amoc['product-type'] == "O"){
                    console.log(obj.product.amoc.identifier);
                } else {

                    var area = obj.product.forecast.area;
                    for (var i = 0, len = area.length; i < len; i++) {
                        area[i]._id = area[i].aac + '_';
                        area[i].identifier = obj.product.amoc.identifier;
                        if (area[i]['forecast-period'] != undefined && area[i]['forecast-period'] instanceof Array) {
                            for (var j = 0, leng = area[i]['forecast-period'].length; j < leng; j++) {

                                var entry = JSON.parse(JSON.stringify(area[i]));
                                entry['forecast-period'] = area[i]['forecast-period'][j];

                                formatForecastPeriod(entry);
                                upsertForecast(db, entry);
                            }
                        } else if (area[i]['forecast-period'] != undefined) {
                            formatForecastPeriod(area[i]);
                            upsertForecast(db, area[i]);
                        } else {
                            /** @todo could be a storm warning */
                        }
                    }
                }
            });
        }
    });
});

function upsertForecast(db, d) {
    db.collection('land_forecast').updateOne({"_id": d._id},d,{upsert:true}, function(err, results){
        if(err){
            console.error(new Error("Failed upserting forecast: " + err));
        }
    });
}

function formatForecastPeriod(entry){

    entry._id += entry['forecast-period']['start-time-local'].slice(0, 10);
    entry['date'] = entry['forecast-period']['start-time-local'].slice(0, 10);

    if (entry['forecast-period']['element'] != undefined && entry['forecast-period']['element'] instanceof Array) {
        for(var t = 0; t < entry['forecast-period']['element'].length;t++) {
            entry[entry['forecast-period']['element'][t]['type']] = entry['forecast-period']['element'][t]['description'];
            if(entry['forecast-period']['element'][t]['units'] != null)
                entry[entry['forecast-period']['element'][t]['type']+'_units'] = entry['forecast-period']['element'][t]['units'];
        }
    }

    if (entry['forecast-period']['text'] != undefined && entry['forecast-period']['text'] instanceof Array) {
        for(var t = 0; t < entry['forecast-period']['text'].length;t++) {
            entry[entry['forecast-period']['text'][t]['type']] = entry['forecast-period']['text'][t]['description'];
        }
    }
    delete entry['forecast-period'];


}
