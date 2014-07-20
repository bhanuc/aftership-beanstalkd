var _ = require('underscore');
var mongoose = require('mongoose');
mongoose.connect(process.env.MONGOLAB_URI || 'mongodb://localhost/aftership');
var Schema = mongoose.Schema;

var courier = require('./courier');
var workqueue = require('./workqueue');

// MongoDB schema for each 'tracking'
var trackingSchema = new Schema({
  slug: String,
  tracking_number: String,
  checkpoints: Array
});
var Tracking = mongoose.model('Tracking', trackingSchema);

// save tracking to database
var saveToDB = exports.saveToDB = function(tracking_result){

  var newTracking = new Tracking(tracking_result);
  
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
              }
            });
        }else{
          newTracking.save(function(err, data){
            if(err){
              console.error(err);
            }
          });
        }
      }
    });
};
