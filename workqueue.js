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

  producerClient.put(1, delay, 2, JSON.stringify({
    slug: slug,
    tracking_number: tracking_number
  }),
  function(err, jobid){
    console.log('put ' + jobid);
    // if no job in queue, restart reserve loop
    // consumerClient.peek_ready(function(err, jobid, payload){
    //   console.log('peek_ready');
    //   if(err){
    //     reserve();
    //   }else{
    //   }
    // });
  });
};

var reserve = function(){
  console.log('reserve');
  // consumerClient.peek_ready(function(err, jobid, payload){
  //   if(jobid){
      // has ready jobs
      consumerClient.use('aftership', function(){});

      consumerClient.reserve(function(err, jobid, payload){

        // book next reserve; 0.5s per connection
        setTimeout(function(){
          // reserve next job (and make http request) after 0.5s
          reserve();
        }, 200);

        if(jobid){
          console.log('reserved ' + jobid + ': ' + payload);
          var payloadData = JSON.parse(payload);

          var startTime = new Date();

          // // work simulator
          // setTimeout(function(){
          //   console.log('DONE');
          //   consumerClient.use('aftership', function(){});

          //   consumerClient.destroy(jobid, function(){
          //     console.log('destroyed ' + jobid);
          //   });
          // }, Math.random() * 1000);          
          // send http request
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
              consumerClient.destroy(jobid, function(){
                console.log('destroyed ' + jobid);            
              });
            },

            // no parcel data or error
            function(err){
              consumerClient.destroy(jobid, function(){
                console.log('destroyed and 3hr replant ' + jobid);
              });
              addRequestToQueue(payloadData.slug, payloadData.tracking_result, 10800);
            }
          );
        }


      });
  //   }
  // });
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

var peek = function(){
  consumerClient.peek_ready(function(err, jobid, payload) {
    if(err){
      console.log('regularPeek error: ' + err);
    }else{    
      console.log('regularPeek: ready ' + jobid);
    }
  });
};

// setInterval(regularPeek, 12000);

var producerClient = new fivebeans.client('127.0.0.1', 11300);

producerClient
  .on('connect', function(){
    console.log('producerClient connect');
    producerClient.use('aftership', function(err, tubename){
      console.log('use ' + tubename);
    });

    setTimeout(function(){
      _.each(trackings, function(tracking){
        addRequestToQueue(tracking[0], tracking[1]);
      });
    }, 50);

    setTimeout(function(){
      _.each(trackings, function(tracking){
        addRequestToQueue(tracking[0], tracking[1]);
      });
    }, 15000);
  })
  .on('error', function(err){
    console.error(err);
  })
  .on('close', function(){
    console.log('close');
  })
  .connect();
