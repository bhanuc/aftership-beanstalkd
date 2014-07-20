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

  // producerClient.use(slug, function(){});
  producerClient.use(slug, function(){});

  // HACK: set ttr to 0 (ttr is key to responsiveness of new task; blocking?)
  producerClient.put(1, delay, 0, JSON.stringify({
    slug: slug,
    tracking_number: tracking_number
  }),
  function(err, jobid){
    console.log('put ' + jobid);
  });
};

var reserve = function(client){
  console.log('reserve');

  client.reserve(function(err, jobid, payload){
    console.log('reserved ' + jobid + ': ' + payload);
    // book next reserve; 0.5s per connection
    setTimeout(function(){
      // reserve next job (and make http request) after 0.5s
      reserve(client);
    }, 200);

    // HACK: destroy job immediately after starting
    client.destroy(jobid, function(){
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
          var logText = 'Slug: ' + payloadData.slug
            + ', Tracking Number: ' + payloadData.tracking_number
            + ', Time Taken: ' + timeTaken + 'ms' + '\n'
          fs.appendFile('jobslog.txt', logText, function(err){
            if(err){console.error('LOG ERROR: ' + err);}
          });

          // destroy job after getting tracking result?
        },

        // no parcel data or error
        function(err){
          // destroy job here?

          // add new job after 3 hours
          addRequestToQueue(payloadData.slug, payloadData.tracking_result, 10800);
        }
      );
    }
  });

};

var producerClient = new fivebeans.client('127.0.0.1', 11300);

producerClient
  .on('connect', function(){
    console.log('producerClient connect');

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

var dpdukConsumerClient = new fivebeans.client('127.0.0.1', 11300);
dpdukConsumerClient.on('connect', function(){
  console.log('dpdukConsumerClient connect');
  dpdukConsumerClient.watch('dpduk', function(){});
  reserve(dpdukConsumerClient);
})
.on('error', function(err){
  console.error(err);
})
.on('close', function(){
  console.log('close');
})
.connect();

var hkpostConsumerClient = new fivebeans.client('127.0.0.1', 11300);
hkpostConsumerClient.on('connect', function(){
  console.log('hkpostConsumerClient connect');
  hkpostConsumerClient.watch('hkpost', function(){});
  reserve(hkpostConsumerClient);
})
.on('error', function(err){
  console.error(err);
})
.on('close', function(){
  console.log('close');
})
.connect();

var uspsConsumerClient = new fivebeans.client('127.0.0.1', 11300);
uspsConsumerClient.on('connect', function(){
  console.log('uspsConsumerClient connect');
  uspsConsumerClient.watch('usps', function(){});
  reserve(uspsConsumerClient);
})
.on('error', function(err){
  console.error(err);
})
.on('close', function(){
  console.log('close');
})
.connect();
