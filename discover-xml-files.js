/**
 * This is a script to fetch a list of forecast documents from the BOM and update the list of documents in the list
 * of files in MongoDB. The purpose of the script is to discover documents that are only occasionally available.
 *
 * @param ID Pass as an argument when running the script the BOM ID for the document
 */
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var mongourl = 'mongodb://localhost:27017/weatherdb';

var JSFtp = require("jsftp");
var ftp = new JSFtp({host: "ftp.bom.gov.au"});

ftp.ls("/anon/gen/fwo/.", function(err, res) {

  MongoClient.connect(mongourl, function (err, db) {
    if (err) console.log('Unable to connect to the mongoDB server. Error:', err);

    var counterA = 0, counterB = 0;
    res.forEach(function(file, index, array) {

      if(file.name.slice(file.name.length-8,file.name.length) == 'amoc.xml') true;
      else if(file.name.slice(file.name.length-7,file.name.length) == 'cap.xml') true;
      else if(file.name.slice(file.name.length-3,file.name.length) == 'xml') {
        counterA++; // Counts the number of files found

        /** If the ID is not in the database then add it */
        var bomID = file.name.slice(0,file.name.length-4);
        if(array.length == 1+index) { end = bomID; }

        db.collection('file').updateOne({"bomId":bomID}, { $set: {"bomId":bomID, "last_found": new Date(), "next_update": new Date()}}, {upsert:true}, function(err, result){
          if(err) console.error(new Error("Failed upserting xml file list: " + err));
          counterB++; // Counts the number of files processed
          if(counterA === counterB) { // Terminate the script once the last file name has been upserted
            db.close();
            process.exit(0);
          }
        });
      }
    });
  });
});


