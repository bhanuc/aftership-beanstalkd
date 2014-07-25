var fs = require('fs');
var _ = require('underscore');
var csv = require('csv');

var fivebeans = require('fivebeans');
var RateLimiter = require('limiter').RateLimiter;
var db = require('./db');
var courier = require('./courier');

var trackings = require('./sample_trackings').sampleTrackings;

// common limiter if overall rate limit of 2 per second is enforced
// var limiter = new RateLimiter(2, 'second');

var addRequestToQueue = exports.addRequestToQueue = function(slug, tracking_number, delay){
  // (priority, delay, ttr, payload, cb)
  delay = delay || 0;

  producerClient.use(slug, function(){});

  // HACK: set ttr to 0 (ttr is key to responsiveness of new task; blocking?)
  producerClient.put(1, delay, 0, JSON.stringify({
    slug: slug,
    tracking_number: tracking_number
  }),
  function(err, jobid){
    console.log('put job ' + jobid);
  });
};

var reserve = function(client){
  console.log('reserve');

  client.reserve(function(err, jobid, payload){
    console.log('reserved job ' + jobid + ': ' + payload);

    client.availableTokens--;

    // HACK: use an 1ms setTimeout to get the next client.reserve triggered
    // (somehow does not work without this...)
    setTimeout(function(){
      if(client.availableTokens > 0){
        // if the token bucket of 20 is not full yet, reserve job immediately
        reserve(client);
      }else{
        // if bucket is full, throttle job according to refill rate
        client.limiter.removeTokens(1, function(err, remainingRequests){
          reserve(client);
        });
      }
    }, 1);

    // HACK: destroy job immediately after starting
    client.destroy(jobid, function(){
      console.log('destroyed job ' + jobid);            
    });

    if(jobid){
      var payloadData = JSON.parse(payload);
      var startTime = new Date();
    
      courier[payloadData.slug](payloadData.tracking_number,
        // successfully received parcel data
        function(result){
          var tracking_result = _.extend(result, {
            slug: payloadData.slug,
            tracking_number: payloadData.tracking_number
          });
          db.saveToDB(tracking_result);
          console.log('saved to DB job ' + jobid);

          var endTime = new Date();
          var timeTaken = endTime - startTime;
          var logText = 'Job ' + jobid
            + ', Slug: ' + payloadData.slug
            + ', Tracking Number: ' + payloadData.tracking_number
            + ', Time Taken: ' + timeTaken + 'ms\n'
          fs.appendFile('jobslog.txt', logText, function(err){
            if(err){console.error('LOG ERROR: ' + err);}
          });
        },

        // no parcel data or error
        function(err){
          // add back the same job which is delayed for 3 hours
          console.log('delayed task for failed request');
          addRequestToQueue(payloadData.slug, payloadData.tracking_result, 10800);
        }
      );
    }
  });
};

// use object to keep track of current couriers with its own client
var couriers = {};

// use a producer client to put all the trackings onto the work queue
var producerClient = new fivebeans.client('127.0.0.1', 11300);
producerClient
  .on('connect', function(){
    console.log('producerClient connect');

    _.each(trackings, function(tracking){
      if(!(tracking[0] in couriers)){
        couriers[tracking[0]] = true;
        // dynamically add courier consumer client when new courier is found on list
        addCourierClient(tracking[0]);
      }
      addRequestToQueue(tracking[0], tracking[1]);
    });
  })
  .on('error', function(err){
    console.error(err);
  })
  .on('close', function(){
    console.log('close');
  })
  .connect();

// function to add new courier consumer client to consume the courier's jobs
var addCourierClient = function(courier){
  var consumerClient = new fivebeans.client('127.0.0.1', 11300);
  consumerClient.on('connect', function(){
    console.log(courier + ' consumerClient connect');
    consumerClient.watch(courier, function(){});
    this.availableTokens = 20;
    // each courier has its own private limiter of 2 requests per second
    this.limiter = new RateLimiter(2, 'second');
    reserve(consumerClient);
  })
  .on('error', function(err){
    console.error(err);
  })
  .on('close', function(){
    console.log('close');
  })
  .connect();
};
