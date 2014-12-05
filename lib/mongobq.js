'use strict';

var Mongo = require('./mongo');
var GCP = require('./gcp');
var uuid = require('uuid');
var async = require('async');
var through2 = require('through2');
var zlib = require('zlib');
var path = require('path');
var moment = require('moment');
var util = require('util');

function MongoBQ(opts) {
  this.opts = opts;
  this.mongo = new Mongo(opts);
  this.gcp = new GCP(opts);

  var directory = opts.directory || '';
  var filename = util.format("%s_%s.json.gz", GCP.safeName(opts.collection), moment().format('YYYYMMDDHHmmss'));

  this.file = this.gcp.storageFile({
    bucket: opts.bucket,
    path: path.join(directory, filename)
  });

  this.table = this.gcp.bigqueryTable({
    project: opts.project,
    dataset: opts.dataset,
    table: opts.table || GCP.safeName(opts.collection)
  });
}

MongoBQ.prototype.run = function () {
  var self = this;

  console.log("Import '%s' collection into '%s'", self.opts.collection, this.table.fullName());
  console.log("Query: %s", JSON.stringify(self.opts.query));

  async.waterfall([
    function (next) {
      console.log("Connecting to %s", self.mongo.mongodbUri);
      self.mongo.connect(next);
    },
    function (next) {
      process.stdout.write("Check bucket '" + self.file.bucket().name + "' ... ");
      self.file.createBucket(function (err, created) {
        if (err) return next(err);
        if (created) {
          console.log("not exists, created new bucket", self.file.bucket().name);
        } else {
          process.stdout.write('already exists');
        }
        console.log();
        next();
      });
    },
    function (next) {
      console.log("Upload to '%s'", self.file.fullPath());
      self.upload(next);
    },
    function (schema, next) {
      console.log("Load '%s' into '%s'", self.file.fullPath(), self.table.fullName());
      var f = function () {
        self.file.exists(function (err, exists) {
          if (exists) {
            self.load(schema, next);
          } else {
            setTimeout(f, 5000);
          }
        });
      };
      setTimeout(f, 0);
    },
    function (job, next) {
      console.log('Load start: job id = ', job.id);
      self.wait(job, next);
    },
    function (jobResult, next) {
      self.clean(next);
    }
  ], function (err) {
    self.mongo.close();

    if (err) {
      console.error(err.stack);
      process.exit(1);
    }
  });
};

MongoBQ.prototype.upload = function (next) {
  var self = this;
  var count = 0;
  var schema = {};
  var stream = self.mongo.stream();

  function onEnd(err) {
    if (err) return next(err);
    if (count === 0) {
      return next(new Error('No objects found'));
    }
    console.log("Export %s objects", count);
    next(null, schema);
  }

  stream
    .pipe(through2.obj(function (data, encoding, cb) {
      data.id = data._id;

      var out = {};
      var keys = Object.keys(data);
      var len = keys.length;
      for (var i = 0; i < len; ++i) {
        var k = keys[i];
        var v = data[k];

        if (k === '_id') {
          continue;
        }

        var col = GCP.safeName(k);
        out[col] = self.table.value(v);
        if (!schema[col]) {
          schema[col] = self.table.column(k, v);
        }
      }
      this.push(JSON.stringify(out) + '\n');
      count++;
      cb();
    }))
    .pipe(zlib.createGzip())
    .pipe(self.file.createWriteStream())
    .on('finish', onEnd)
    .on('error', next);
};

MongoBQ.prototype.load = function (schema, next) {
  var loadConfig = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    writeDisposition: 'WRITE_TRUNCATE',
    ignoreUnknownValues: true
  };
  this.table.load(this.file, schema, loadConfig, next);
};

MongoBQ.prototype.wait = function (job, next) {
  if (this.opts.async) {
    return process.nextTick(next);
  }

  process.stdout.write('Waiting job completion ');
  var f = function () {
    job.getMetadata(function (err, metadata) {
      if (err) return next(err);

      process.stdout.write('.');
      if (metadata.status.state === 'DONE') {
        next(null, handleJobResult(metadata));
      } else {
        setTimeout(f, 5000);
      }
    });
  };
  setTimeout(f, 0);
};

MongoBQ.prototype.clean = function (next) {
  if (this.opts.autoclean && !this.opts.async) {
    this.file.clean(next);
  } else {
    process.nextTick(next);
  }
};

function handleJobResult(metadata) {
  var status = metadata.status;
  if (status.errors && status.errors.length !== 0) {
    process.stdout.write('FAILURE\n');
    console.log();
    console.log('Errors encountered during job execution.');
    console.log('Failure deitals:');
    status.errors.forEach(function (err) {
      if (err.location) {
        console.log(' - %s: %s', err.location, err.message);
      } else {
        console.log(err.message);
      }
    });
  } else {
    process.stdout.write('SUCCESS\n');
  }

  return metadata;
}

module.exports = MongoBQ;
