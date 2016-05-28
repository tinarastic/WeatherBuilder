/**
 * This is a script to fetch forecasts from BOM and save to the MongoDB WeatherDB.
 *
 * @param ID Pass as an argument when running the script the BOM ID for the document
 */

var mongodb = require('mongodb');
var JSFtp = require("jsftp");
var parser = require('xml2json');

var MongoClient = mongodb.MongoClient;
var mongourl = 'mongodb://localhost:27017/weatherdb';
var ftp = new JSFtp({host: "ftp.bom.gov.au", debugMode:true});

var args = process.argv.slice(2);
if(args.length == 0) {
  console.log('Please provide a valid ID');
  process.exit(1);
}

/**
 * Fetch the forecast document for the FTP site in XML format
 */
ftp.get('/anon/gen/fwo/' + args[0] + '.xml', function(err, socket) {

  if (err) {
    /* @todo: log errors to file or database */
	console.error('Error accessing file');
    process.exit(1);
	return;
  }

  socket.on("data", function(d) {

    try {
      var data = parser.toJson(d.toString(), {reversible: false, object: false});
    } catch(err) {
      console.error('Error parsing the file, something went wrong: ' + err);
      process.exit(1);
    }

    var str = data.replace(/\$t/g,"description");
    var obj = JSON.parse(str);
    var area = obj.product.forecast.area;
    for (var i = 0, len = area.length; i < len; i++) {

      area[i]._id = args[0] + '-';
      area[i]._id += area[i].aac  + '-';

      if (area[i]['forecast-period'] != undefined && area[i]['forecast-period'] instanceof Array) {
        for (var j = 0, leng = area[i]['forecast-period'].length; j < leng; j++) {

          var entry = JSON.parse(JSON.stringify(area[i]));
          entry._id += area[i]['forecast-period'][j]['start-time-local'].slice(0, 10);
          entry['forecast-period'] = area[i]['forecast-period'][j];

          if(entry['forecast-period']['text'] != undefined) {
            entry['entry_type'] = entry['forecast-period']['text']['type'];
            entry['forecast'] = entry['forecast-period']['text']['description'];
            delete entry['forecast-period']['text'];
          }

          insertWeather(entry);
        }
      } else {
        insertWeather(area[i]);
      }



    }

  });

  socket.on("close", function(hadErr) {
    if (hadErr)
      console.error('There was an error retrieving the file.');
  });
  socket.resume();
});

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

    db.collection('raw_forecast').updateOne({"_id": d._id},d,{upsert:true}, function(err, results){

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



