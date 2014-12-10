'use strict';

var Mongo = require('./mongo');
var GCP = require('./gcp');
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

  var self = this;
  var bucketPath = opts.path || '';
  var suffixFormat = opts.suffix || 'YYYYMMDDHHmmss';
  var filename = util.format("%s_%s.json", GCP.safeName(opts.collection), moment().format(suffixFormat));
  if (opts.compress) {
    filename += '.gz';
  }

  self.file = function () {
    return self.gcp.storageFile({
      bucket: opts.bucket,
      path: path.join(bucketPath, filename)
    });
  };

  self.table = function () {
    return self.gcp.bigqueryTable({
      project: opts.project,
      dataset: opts.dataset,
      table: opts.table || GCP.safeName(opts.collection)
    });
  };
}

MongoBQ.prototype.run = function () {
  var self = this;

  console.log();
  if (self.opts.dryrun) {
    console.log('*** DRYRUN MODE ***');
  }

  console.log("Import '%s' collection into '%s'", self.opts.collection, this.table().fullName());

  var tasks = [
    self.connectToMongoDB.bind(self),
    self.createBucket.bind(self),
    self.uploadToGCS.bind(self),
    self.importIntoBigQuery.bind(self),
    self.waitLoadJob.bind(self),
    self.clean.bind(self)
  ];

  async.waterfall(tasks, function (err, result) {
    self.mongo.close();

    if (err) {
      console.error(err.stack);
      process.exit(1);
    }

    var status = result.status;
    if (status.errors && status.errors.length !== 0) {
      process.exit(1);
    }
    console.log('DONE');
  });
};

MongoBQ.prototype.connectToMongoDB = function (next) {
  console.log("Connecting to %s", this.mongo.mongodbUri);
  this.mongo.connect(next);
};

MongoBQ.prototype.createBucket = function (next) {
  var self = this;

  process.stdout.write("Check bucket '" + self.file().bucket().name + "' ... ");
  if (self.opts.dryrun) return next();
  self.file().createBucket(function (err, created) {
    if (err) return next(err);
    if (created) {
      console.log("not exists, created new bucket", self.file().bucket().name);
    } else {
      console.log('already exists');
    }
    next();
  });
};

MongoBQ.prototype.uploadToGCS = function (next) {
  var self = this;
  var count = 0;
  var autoSchemaDetection = !self.opts.schema;
  var schema = self.opts.schema || [];
  var fields = self.opts.fields || [];
  var stream = self.mongo.stream()
                .pipe(GCP.BigQueryTable.createJSONStream(schema, autoSchemaDetection))
                .on('data', function (d) { count++; })
                .on('error', next);

  function waitFileVisible(cb) {
    var f = function () {
      self.file().exists(function (err, exists) {
        if (err) return cb(err);
        return exists ? cb(null, schema) : setTimeout(f, 5000);
      });
    };
    setTimeout(f, 0);
  }

  function onEnd(err) {
    if (err) return next(err);
    if (count === 0) {
      return next(new Error('No objects found'));
    }
    console.log("%s objects found", count);
    console.log("Uploaded to '%s'", self.file().fullPath());

    if (self.opts.dryrun) {
      next(null, schema);
    } else {
      waitFileVisible(next);
    }
  }

  console.log("Query: %s", JSON.stringify(self.opts.query));
  console.log("Fields: %s", JSON.stringify(self.opts.fields));

  if (self.opts.dryrun) {
    stream.on('end', onEnd);
  } else {
    if (self.opts.compress) {
      stream = stream.pipe(zlib.createGzip());
    }
    stream
      .pipe(self.file().createWriteStream())
      .on('error', next)
      .on('finish', onEnd);
  }
};

MongoBQ.prototype.importIntoBigQuery = function (schema, next) {
  var loadConfig = {
    sourceFormat: 'NEWLINE_DELIMITED_JSON',
    writeDisposition: 'WRITE_TRUNCATE',
    ignoreUnknownValues: true,
    schema: {
      fields: schema
    }
  };

  console.log("Load '%s' into '%s'", this.file().fullPath(), this.table().fullName());
  console.log("=== Load config ===");
  console.log(JSON.stringify(loadConfig, null, '  '));
  console.log("===================");
  console.log();

  if (this.opts.dryrun) return next(null, null);
  this.table().load(this.file(), loadConfig, next);
};

MongoBQ.prototype.waitLoadJob = function (job, next) {
  if (this.opts.dryrun) return next(null, null);
  if (this.opts.async) return next(null, null);

  console.log('Load start: job id = ', job.id);
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

MongoBQ.prototype.clean = function (jobResult, next) {
  if (this.opts.dryrun) return next();

  var self = this;
  if (self.opts.autoclean && !self.opts.async) {
    console.log('Cleaning... ');
    self.file().clean(function (err) {
      if (err) return next(err);
      console.log(' - delete %s', self.file().fullPath());
      next(null, jobResult);
    });
  } else {
    next(null, jobResult);
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
