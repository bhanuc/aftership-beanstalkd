var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var trackingSchema = new Schema({
  slug: String,
  tracking_number: String,
  checkpoints: Array
});

var Tracking = mongoose.model('Tracking', trackingSchema);
