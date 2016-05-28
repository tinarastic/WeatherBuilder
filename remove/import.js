/**
 * This is a script to fetch forecasts from BOM and save to the MongoDB WeatherDB.
 * @deprecated senda
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
process.exit(1);

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
      var data = parser.toJson(d.toString(), {reversible: false, object: true});
    } catch(err) {
      console.error('Error parsing the file, something went wrong: ');
      console.error(err);
      process.exit(1);
    }

    var areas = data.product.forecast.area;

    /**
     * Iterating through the 'areas', not all areas have forecasts
     */
    for (var i = 0, len = areas.length; i < len; i++){

      var location = areas[i]['description'];
      var aac = areas[i]['aac'];

      /**
       * The forecast is in an array of periods, each being the forecast for a particular day
       */
      if (areas[i]['forecast-period'] != undefined) {
        var period = areas[i]['forecast-period'];
        if (period instanceof Array) { // This is the forecast
          for (var j = 0, leng = period.length; j < leng; j++) {

            var date = period[j]['start-time-local'].slice(0, 10);
            var document = {
              "_id": data.product.amoc.identifier + '-' + date + '-' + period[j].text.type,
              "bomId": data.product.amoc.identifier,
              "date": date,
              "aac": aac,
              "type": period[j].text.type,
              "location": location
            };

            var forecastArray = period[j].text['$t'].split(/[a-zA-Z0-9]+: /).reverse();
            forecastArray.pop();
            var key = period[j].text['$t'].match(/Winds:|Seas:|Weather:/g).reverse();
            for(var k=0; k < forecastArray.length; k++){
              document[key[k].slice(0,key[k].length-1)] = forecastArray[k];
            }

            insertWeather(document);
          }
        }

        /**
         * There is also a generic situation description that may be valuable
         */
        else if (period.text.type == 'synoptic_situation') {
          var date = period['start-time-local'].slice(0, 10);

          insertWeather({
            "_id": data.product.amoc.identifier + '-' + date + '-' + period.text.type,
            "bomId": data.product.amoc.identifier,
            "date": date,
            "aac": aac,
            "type": period.text.type,
            "location": location,
            "forecast": period.text['$t']
          });
        }

      }
      else if(areas[i]['warning-summary'] != undefined) {
        var date = areas[i]['warning-summary']['start-time-local'].slice(0, 10);

        insertWeather({
          "_id": data.product.amoc.identifier + '-' + date + '-' + areas[i]['warning-summary'].type,
          "bomId": data.product.amoc.identifier,
          "date": date,
          "aac": aac,
          "type": areas[i]['warning-summary'].type,
          "location": location,
          "forecast": areas[i]['warning-summary']['$t']
        });
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



