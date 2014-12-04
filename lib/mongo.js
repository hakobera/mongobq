var util = require('util');
var MongoClient = require('mongodb').MongoClient;

var SCHEMA = 'mongodb';

function buildMongodbUri(opts) {
  if (opts.username) {
    return util.format("%s://%s:%s@%s:%d/%s", SCHEMA, opts.username, opts.password, opts.host, opts.port, opts.database);
  } else {
    return util.format("%s://%s:%d/%s", SCHEMA, opts.host, opts.port, opts.database);
  }
}

function Mongo(opts) {
  this.opts = opts;
  this.mongodbUri = buildMongodbUri(opts);
  this.collection = opts.collection;
}

Mongo.prototype.connect = function (callback) {
  MongoClient.connect(this.mongodbUri, callback);
};

module.exports = Mongo;
