var express = require('express');

var app = express();
var courier = require('./courier');

//basic logger
app.use(function(req, res, next){
  console.log(req.method + ': ' + req.url);
  next();
});

app.listen(process.env.PORT || 5555);
