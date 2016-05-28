/**
 * This is a script to fetch forecasts from BOM and save to the MongoDB WeatherDB.
 *
 * @param ID Pass as an argument when running the script the BOM ID for the document
 */

var JSFtp = require("jsftp");
var ftp = new JSFtp({host: "ftp.bom.gov.au", debugMode:true});

var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;

var parser = require('xml2json');
var counter = 0;

function fetchDocument(document) {

  ftp.on('jsftp_debug', function(eventType, data) {
    console.log('DEBUG: ', eventType);
    console.log(JSON.stringify(data, null, 2));
  });

  console.log(document);
  ftp.get('/anon/gen/fwo/' + document + '.xml', function (err, socket) {

    if (err) console.error('Error accessing file');

    socket.on("data", function (d) {

      console.log('Got '+document);

      try {
        var data = parser.toJson(d.toString(), {reversible: false, object: false});
      } catch (err) {
        console.error('Error processing the file ' + document + ', something went wrong when converting the XML to a ' +
            'JSON formatted string: ' + err + '. Try again later.');
        return;
      }

      var str = data.replace(/\$t/g, "description");
      var obj = JSON.parse(str);
      if(obj.product.forecast == undefined) {
        console.error('Parsing failed, error in: ');
        return;
      }
      var area = obj.product.forecast.area;

      updateList({
        "identifier":obj.product.amoc.identifier,
        "region":obj.product.amoc.source.region,
        "next":obj.product.amoc['next-routine-issue-time-local'].description,
        "status":obj.product.amoc.status,
        "product-type":obj.product.amoc['product-type'],
        "phase":obj.product.amoc.phase
      });

      for (var i = 0, len = area.length; i < len; i++) {

        area[i]._id = document + '-';
        area[i]._id += area[i].aac + '-';
        area[i].identifier = obj.product.amoc.identifier;

        if (area[i]['forecast-period'] != undefined && area[i]['forecast-period'] instanceof Array) {
          for (var j = 0, leng = area[i]['forecast-period'].length; j < leng; j++) {

            var entry = JSON.parse(JSON.stringify(area[i]));
            entry._id += area[i]['forecast-period'][j]['start-time-local'].slice(0, 10);
            entry['forecast-period'] = area[i]['forecast-period'][j];

            if (entry['forecast-period']['text'] != undefined) {
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

    socket.on("end",function(){
      console.log('Ending now');

    });
    socket.on("close", function (hadErr) {
      console.log('Closing now');
      if (hadErr) console.error('There was an error retrieving the file.');


    });
    socket.resume();
  });

  var key = documents.indexOf(document);
  if (documents.length <= 1+key) // process.exit(0);
    return;
  else fetchDocument(documents[key+1]);

}

/**
 * Connect to MongoDB and insert the forecast as a JSON object
 * @param d Object The json object to upsert
 */
function insertWeather(d){
  MongoClient.connect('mongodb://localhost:27017/weatherdb', function (err, db) {
    if (err) console.error('Unable to connect to the mongoDB server. Error:', err);

    db.collection('new_forecast').updateOne({"_id": d._id},d,{upsert:true}, function(err){
      if(err) console.error(new Error("Failed upserting forecast: " + err));
      db.close();
    });

  });
}

function updateList(d){
  MongoClient.connect('mongodb://localhost:27017/weatherdb', function (err, db) {
    if (err) console.error('Unable to connect to the mongoDB server. Error:', err);

    db.collection('marine').updateOne({"identifier": d.identifier}, d,{upsert:true}, function(err, result){
      if(err) console.error(new Error("Failed upserting marine: " + err));
      else console.log("Updated " + d.identifier);
      db.close();
    });

  });

}

var documents = ['IDV10460','IDV10465','IDV10461','IDV19300','IDV10252'];
fetchDocument(documents[0]);



