var fs = require('fs');
var _ = require('underscore');
var csv = require('csv');

var fivebeans = require('fivebeans');
var db = require('./db');
var courier = require('./courier');

var trackings = require('./sample_trackings').sampleTrackings;

var addRequestToQueue = exports.addRequestToQueue = function(slug, tracking_number, delay){
  // (priority, delay, ttr, payload, cb)
  delay = delay || 0;
  producerClient.use('aftership', function(){});

  // HACK: set ttr to 0 (ttr is key to responsiveness of new task; blocking?)
  producerClient.put(1, delay, 0, JSON.stringify({
    slug: slug,
    tracking_number: tracking_number
  }),
  function(err, jobid){
    console.log('put ' + jobid);
  });
};

var reserve = function(){
  console.log('reserve');
  consumerClient.use('aftership', function(){});

  consumerClient.reserve(function(err, jobid, payload){
    console.log('reserved ' + jobid + ': ' + payload);
    // book next reserve; 0.5s per connection
    setTimeout(function(){
      // reserve next job (and make http request) after 0.5s
      reserve();
    }, 200);

    // HACK: destroy job immediately after starting
    consumerClient.destroy(jobid, function(){
      console.log('destroyed ' + jobid);            
    });

    if(jobid){
      var payloadData = JSON.parse(payload);
      var startTime = new Date();
    
      // work: send http request
      courier[payloadData.slug](payloadData.tracking_number,
        // successfully received parcel data
        function(result){
          var tracking_result = _.extend(result, {
            slug: payloadData.slug,
            tracking_number: payloadData.tracking_number
          });
          db.saveToDB(tracking_result);

          var endTime = new Date();
          var timeTaken = endTime - startTime;
          console.log('time taken for ' + jobid + ': ' + timeTaken);

          // destroy job after getting tracking result
          // consumerClient.destroy(jobid, function(){
          //   console.log('destroyed ' + jobid);            
          // });
        },

        // no parcel data or error
        function(err){
          // consumerClient.destroy(jobid, function(){
          //   console.log('destroyed and 3hr replant ' + jobid);
          // });

          // add new job after 3 hours
          addRequestToQueue(payloadData.slug, payloadData.tracking_result, 200);
        }
      );
    }
  });
};

var consumerClient = new fivebeans.client('127.0.0.1', 11300);

consumerClient
  .on('connect', function(){
    console.log('consumerClient connect');
    consumerClient.use('aftership', function(err, tubename){
      console.log('use ' + tubename);
    });
    consumerClient.watch('aftership', function(err, tubename){
      console.log('watch ' + tubename);
    });

    reserve();
  })
  .on('error', function(err){
    console.error(err);
  })
  .on('close', function(){
    console.log('close');
  })
  .connect();

var producerClient = new fivebeans.client('127.0.0.1', 11300);

producerClient
  .on('connect', function(){
    console.log('producerClient connect');
    producerClient.use('aftership', function(err, tubename){
      console.log('use ' + tubename);
    });

    _.each(trackings, function(tracking){
      addRequestToQueue(tracking[0], tracking[1]);
    });

    // setTimeout(function(){
    //   _.each(trackings, function(tracking){
    //     addRequestToQueue(tracking[0], tracking[1]);
    //   });
    // }, 15000);
  })
  .on('error', function(err){
    console.error(err);
  })
  .on('close', function(){
    console.log('close');
  })
  .connect();
