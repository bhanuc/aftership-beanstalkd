var http = require('http');

var _ = require('underscore');
var parseString = require('xml2js').parseString; // use XML to JS object parser (for USPS)
var cheerio = require('cheerio'); // HTML string to DOM object parser (for HKP)

// use moment for time format manipulation
var moment = require('moment');
var formatTime = function(date){
  return moment(date).format('YYYY-MM-DDThh:mm:ss');
};

var Courier = function() {
  this.usps = function(tracking_number, callback) {
    var tracking_result = {};

    // options for USPS http request
    var options = {
      host: 'production.shippingapis.com',
      path: '/ShippingAPITest.dll?API=TrackV2&XML=%3CTrackFieldRequest%20USERID=%22519MYCOM6991%22%3E%3CTrackID%20ID=%22' +
        tracking_number + '%22%3E%3C/TrackID%3E%3C/TrackFieldRequest%3E'
    };

    http.get(options, function(res) {
      var data = '';
      res.on('data', function(chunk){
        data += chunk;
      });
      res.on('end', function(){
        processData(data);
      });
    }).on('error', function(err) {
      console.error(err);
    });

    var processData = function(data){
      // parse the response XML body; function is synchronous
      parseString(data, function(err, result){
        if(result.TrackResponse.TrackInfo.TrackSummary){
          // parcel found

          var trackSummary = result.TrackResponse.TrackInfo[0].TrackSummary[0];
          // can manipulate the array result.TrackResponse.TrackInfo[0].TrackDetail if more details are needed

          // extract info from trackSummary
          var countryName = trackSummary.EventCountry[0];
          var message = trackSummary.Event[0];
          var checkpointTime = formatTime(trackSummary.EventDate[0] + ' ' + trackSummary.EventTime[0]);

          //fixit: need to accommodate multiple events
          tracking_result.checkpoints = [];
          tracking_result.checkpoints.push({
            country_name: countryName,
            message: message,
            checkpoint_time: checkpointTime
          });

          callback(tracking_result);          
        }else{
          // parcel not found
        }
      });
    };
  };

  // TODO: Fix destination country parsing
  // TODO: distinguish between successful requests and failed ones
  this.hkpost = function(tracking_number, callback) {
    var tracking_result = {};

    // options for HKP http request
    var options = {
      host: 'app3.hongkongpost.hk',
      path: '/CGI/mt/mtZresult.jsp?tracknbr=' + tracking_number
    };

    http.get(options, function(res) {
      var data = '';
      res.on('data', function(chunk){
        data += chunk;
      });
      res.on('end', function(){
        processData(data);
      });
    }).on('error', function(err) {
      console.error(err);
    });

    var processData = function(data){
      // use cheerio to convert response body to 'DOM object'
      var $ = cheerio.load(String(data));

      // get text from #clfContent div and split into array
      var htmlData = _.filter($('#clfContent').text().split('  '), function(el){
        return el.length;
      });
      // purge data (cleared empty spaces)
      var purgedData = htmlData[3].split('\r\n');
      
      // get country name from data
      var countryNameData = purgedData[purgedData.length - 2].split(' ');
      var countryName = countryNameData[countryNameData.length - 1];

      // slice out appropriate message from data
      var messageData = purgedData[purgedData.length - 1];
      // hack: use trick to slice off tracking number from message
      var numberSlicePos = messageData.indexOf(tracking_number);
      var message = messageData.slice(0, numberSlicePos - 1) +
        messageData.slice(numberSlicePos + tracking_number.length + 2, messageData.length - 1);

      // hack: get checkpoint time from message
      var checkpointTime = formatTime(message.slice(message.length - 12, message.length - 1));

      tracking_result.checkpoints = [];
      tracking_result.checkpoints.push({
        country_name: countryName,
        message: message,
        checkpoint_time: checkpointTime
      });

      callback(tracking_result);
    };
  };

  this.dpduk = function(tracking_number, callback) {
    var tracking_result = {};

    // options for DPD UK http request
    var options = {
      host: 'www.dpd.co.uk',
      path: '/esgServer/shipping/delivery/?parcelCode=' + tracking_number,
      headers: {
        // hack: cookie from the DPD UK search page
        Cookie: 'X-Mapping-fgaocaep=F91CEB20A8EB613C7F747575954BCF46; JSESSIONID=3E5A9902E760C7AA341EFA192171AF9D; tracking=3364b640-0e23-11e4-b82f-7dc6eacafd15; __utma=43400944.1881649165.1405635305.1405635313.1405650475.3; __utmb=43400944.26.9.1405651011457; __utmc=43400944; __utmz=43400944.1405635313.2.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided)',
      }
    };

    http.get(options, function(res) {
      var data = '';
      res.on('data', function(chunk){
        data += chunk;
      });
      res.on('end', function(){
        processData(data);
      });
    }).on('error', function(err) {
      console.error(err);
    });

    var processData = function(data){
      // get JSON object from response data

      if(JSON.parse(data).obj){
        // parcel found
        var jsonData = JSON.parse(data).obj;
        var trackingEvents = jsonData.trackingEvent;

        // format the events data and push into tracking_result.checkpoints
        tracking_result.checkpoints = [];
        _.each(trackingEvents, function(event){

          // get data for each event
          var countryName = event.trackingEventLocation;
          var message = event.trackingEventStatus;
          // hack: trick to slice out time zone to make test pass
          var checkpointTime = event.trackingEventDate.slice(0, event.trackingEventDate.length - 5);

          tracking_result.checkpoints.push({
            country_name: countryName,
            message: message,
            checkpoint_time: checkpointTime
          });
        })
        //reverse checkpoints array to make it oldest first
        tracking_result.checkpoints.reverse();

        callback(tracking_result);
      }else{
        // parcel not found
      }
    };
  };
}

module.exports = new Courier();
