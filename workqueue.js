var fivebeans = require('fivebeans');
var db = require('./db');

var consumerClient = new fivebeans.client('127.0.0.1', 11300);

console.log(consumerClient);

consumerClient
  .on('connect', function(){
    console.log('consumerClient connect');
    consumerClient.use('testTube', function(err, tubename){
      console.log('use ' + tubename);
    });
    consumerClient.watch('testTube', function(err, tubename){
      console.log('watch ' + tubename);
    });

    var reserve = function(){
      console.log('reserve');
      consumerClient.reserve(function(err, jobid, payload){
        console.log('reserved ' + jobid + ': ' + payload);
        var payloadData = JSON.parse(payload);

        db.fetchTracking(payloadData.slug, payloadData.tracking_number);

        setTimeout(function(){
          console.log('DONE');
          consumerClient.destroy(jobid, function(){
            console.log('destroyed ' + jobid);
            reserve();
          });
        }, 500);

      });
    };
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
    producerClient.use('testTube', function(err, tubename){
      console.log('use ' + tubename);
    });

    var addRequestToQueue = exports.addRequestToQueue = function(slug, tracking_number){
      // (priority, delay, ttr, payload, cb)
      producerClient.put(1, 0, 30, JSON.stringify({
        slug: slug,
        tracking_number: tracking_number
      }),
      function(err, jobid){
        console.log('put ' + jobid);
      });
    };
    setTimeout(function(){
      addRequestToQueue('usps', '9405903699300184125060');
      addRequestToQueue('hkpost', 'RC933607107HK');
      addRequestToQueue('dpduk', '15502370264989N');      
    }, 1000);

  })
  .on('error', function(err){
    console.error(err);
  })
  .on('close', function(){
    console.log('close');
  })
  .connect();
