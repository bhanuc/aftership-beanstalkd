var _ = require('underscore');
var mongoose = require('mongoose');
mongoose.connect(process.env.MONGOLAB_URI || 'mongodb://localhost/aftership');
var Schema = mongoose.Schema;

var courier = require('./courier');

// MongoDB schema for each 'tracking'
var trackingSchema = new Schema({
  slug: String,
  tracking_number: String,
  checkpoints: Array
});
var Tracking = mongoose.model('Tracking', trackingSchema);

// functions for making http requests to fetch tracking data
var fetchUSPS = exports.fetchUSPS = function(tracking_number){
  console.log('usps');

  courier.usps(tracking_number, function(result){
    var newTracking = new Tracking(_.extend(result, {
      slug: 'usps',
      tracking_number: tracking_number
    }));
    saveToDB(newTracking);
  });
};
var fetchHKPOST = exports.fetchHKPOST = function(tracking_number){
  console.log('hkpost');

  courier.hkpost(tracking_number, function(result){
    var newTracking = new Tracking(_.extend(result, {
      slug: 'hkpost',
      tracking_number: tracking_number
    }));
    saveToDB(newTracking);
  });
};
var fetchDPDUK = exports.fetchDPDUK = function(tracking_number){
  console.log('dpduk');

  courier.dpduk(tracking_number, function(result){
    var newTracking = new Tracking(_.extend(result, {
      slug: 'dpduk',
      tracking_number: tracking_number
    }));
    saveToDB(newTracking);
  });
};

// save tracking to database
var saveToDB = function(newTracking){
  
  Tracking.find(
    {tracking_number: newTracking.tracking_number},
    function(err, tracking){
      if(err){
        console.error(err);
      }else{
        if(tracking.length){
          var oldTracking = tracking[0];
          Tracking.update(
            {tracking_number: oldTracking.tracking_number},
            {checkpoints: newTracking.checkpoints},
            function(err, data){
              if(err){
                console.error(err);
              }else{
                console.log('updated data');
              }
            });
        }else{
          newTracking.save(function(err, data){
            if(err){
              console.error(err);
            }else{
              console.log('new tracking saved:');
              console.log(data);
            }
          });
        }
      }
    });
};
