/**
 * This is a script to fetch a list of forecast documents from the BOM and update the list of documents in the list
 * of files in MongoDB
 *
 * @param ID Pass as an argument when running the script the BOM ID for the document
 */

var mongodb = require('mongodb');
var JSFtp = require("jsftp");

var MongoClient = mongodb.MongoClient;
var mongourl = 'mongodb://localhost:27017/weatherdb';
var ftp = new JSFtp({host: "ftp.bom.gov.au"});
var ids = [];

ftp.ls("/anon/gen/fwo/.", function(err, res) {
  res.forEach(function(file) {
    if(file.name.slice(file.name.length-8,file.name.length) == 'amoc.xml') true;
    else if(file.name.slice(file.name.length-7,file.name.length) == 'cap.xml') true;
    else if(file.name.slice(file.name.length-3,file.name.length) == 'xml') ids.push({"bomId":file.name.slice(0,file.name.length-4)});
  });
  insertIds(ids);
});

/**
 * Connect to MongoDB and insert the forecast as a JSON object
 * @param d
 */
function insertIds(d){

  MongoClient.connect(mongourl, function (err, db) {

    if (err) {
      console.log('Unable to connect to the mongoDB server. Error:', err);
      process.exit(1);
    }

    /**
     * If the ID is not in the database then add it
     */
    db.collection('ids').insert(d, function(err, results){

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



