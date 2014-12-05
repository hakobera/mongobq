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
  this.query = opts.query || {};
  this.db = null;
}

Mongo.prototype.connect = function (callback) {
  var self = this;

  MongoClient.connect(this.mongodbUri, function (err, db) {
    if (err) return callback(err);
    self.db = db;
    callback();
  });
};

Mongo.prototype.stream = function () {
  return this.db.collection(this.collection).find(this.query).stream(true);
};

Mongo.prototype.close = function () {
  if (this.db) {
    this.db.close();
  }
};

module.exports = Mongo;
