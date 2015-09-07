'use strict';

var util = require('util');
var path = require('path');
var _ = require('underscore');
var underscoreDeepExtend = require('underscore-deep-extend');
var gcloud = require('gcloud');
var moment = require('moment');
var ObjectID = require('mongodb').ObjectID;
var through2 = require('through2');

_.mixin({deepExtend: underscoreDeepExtend(_)});

function GCP(opts) {
  this.projectSettings = {
    projectId: opts.project
  };

  if (opts.keyfile) {
    this.projectSettings.keyFilename = opts.keyfile;
  }
}

GCP.safeName = function (name) {
  var n = name.replace(/\W/, '_').toLowerCase();
  if (n.charAt(0) === '_') {
    n = n.substr(1);
  }
  return n;
};

GCP.prototype.storageFile = function (opts) {
  return new GCSFile(this.projectSettings, opts);
};

GCP.prototype.bigqueryTable = function (opts) {
  return new BigQueryTable(this.projectSettings, opts);
};

module.exports = GCP;

// Google Cloud Storage

function GCSFile(projectSettings, opts) {
  this.projectSettings = projectSettings;
  this.bucketName = opts.bucket;
  this.path = opts.path;
}

GCSFile.prototype.storage = function () {
  return gcloud.storage(this.projectSettings);
};

GCSFile.prototype.bucket = function () {
  return this.storage().bucket(this.bucketName);
};

GCSFile.prototype.file = function () {
  return this.bucket().file(this.path);
};

GCSFile.prototype.createWriteStream = function () {
  return this.file().createWriteStream({
    resumable: true
  });
};

GCSFile.prototype.fullPath = function () {
  return util.format("gs://%s", path.join(this.bucketName, this.path));
};

GCSFile.prototype.exists = function (callback) {
  this.file().getMetadata(function(err) {
    if (err && err.code === 404) {
      callback(null, false);
    } else if (err) {
      callback(err);
    } else {
      callback(null, true);
    }
  });
};

GCSFile.prototype.createBucket = function (callback) {
  var self = this;
  self.bucket().getMetadata(function(err) {
    if (err && err.code === 404) {
      self.storage().createBucket(self.bucketName, function (err, bucket) {
        callback(err, true);
      });
    } else if (err) {
      process.nextTick(function () {
        callback(err);
      });
    } else {
      process.nextTick(function () {
        callback(null, false);
      });
    }
  });
};

GCSFile.prototype.clean = function (callback) {
  this.file().delete(callback);
};

// Google BigQuery

function BigQueryTable(projectSettings, opts) {
  this.projectSettings = projectSettings;
  this.datasetId = opts.dataset;
  this.tableId = opts.table;
}

BigQueryTable.prototype.bq = function () {
  return gcloud.bigquery(this.projectSettings);
};

BigQueryTable.prototype.dataset = function () {
  return this.bq().dataset(this.datasetId);
};

BigQueryTable.prototype.table = function () {
  return this.dataset().table(this.tableId);
};

BigQueryTable.prototype.fullName = function () {
  return util.format("%s:%s.%s", this.projectSettings.projectId, this.datasetId, this.tableId);
};

BigQueryTable.prototype.metadata = function (callback) {
  this.table().getMetadata(callback);
};

BigQueryTable.prototype.load = function (file, config, callback) {
  this.table().import(file.file(), config, callback);
};

/**
 * Currently not support RECORD object
 */
GCP.BigQueryTable = {
  value: function (v) {
    if (v === null) {
      return null;
    } else if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean' || v instanceof ObjectID) {
      return v;
    } else if (util.isDate(v)) {
      return moment(v).utc().format('YYYY-MM-DD HH:mm:ss');
    } else if (util.isArray(v)) {
      if (v.length === 0) {
        return v;
      } else {
        return v.map(function (item) {
          return GCP.BigQueryTable.value(item);
        }).filter(function (item) {
          return item !== null;
        });
      }
    } else if(v === Object(v)) {
      return v;
    } else {
      return JSON.stringify(v);
    }
  },

  type: function (v) {
    if (util.isDate(v)) {
      return 'TIMESTAMP';
    } else if (typeof v === 'number') {
      return 'FLOAT';
    } else if (typeof v === 'boolean') {
      return 'BOOLEAN';
    } else if(v instanceof ObjectID) {
      return 'STRING';
    } else if (util.isArray(v) && v[0]) {
      return GCP.BigQueryTable.type(v[0]);
    } else if (v === Object(v)) {
      return 'RECORD';
    }
    return 'STRING';
  },

  mode: function (v) {
    if (util.isArray(v)) {
      return 'REPEATED';
    } else {
      return 'NULLABLE';
    }
  },

  column: function (k, v) {
    var colType = GCP.BigQueryTable.type(v);

    if(colType === 'RECORD')
      return {
        name: GCP.safeName(k),
        type: colType,
        mode: GCP.BigQueryTable.mode(v),
        fields: {}
      };
    else
      return {
        name: GCP.safeName(k),
        type: colType,
        mode: GCP.BigQueryTable.mode(v)
      };

  },

  createJSONStream: function (fields, autoSchemaDetection) {
    var schema = {};

    return through2.obj(function (data, encoding, next) {
      var out = GCP.BigQueryTable.parseJSONLine(Object.keys(data), data, autoSchemaDetection);
      schema = _.deepExtend(schema, out.schema);
      this.push(JSON.stringify(out.record) + '\n');
      next();
    }).on('end', function () {
      GCP.BigQueryTable.convertSchemaFields(schema, fields);
    });
  },

  parseJSONLine: function(keys, data, autoSchemaDetection){
    var record = {};
    var schema = {};
    var k, v, col;

    for(var i=0; i<keys.length; i++){
      k = keys[i];
      v = data[k];

      if (k !== '__v' && v !== null) {
        col = GCP.safeName(k);
        var colType = GCP.BigQueryTable.type(v);
        var colMode = GCP.BigQueryTable.mode(v);
        var subRecord;

        if(autoSchemaDetection && !schema[col]) schema[col] = GCP.BigQueryTable.column(k, v);

        if(colMode === 'REPEATED'){
          var j;
          record[col] = [];

          if(colType === 'RECORD'){
            for (j=0; j<v.length; j++){
              if(!_.isEmpty(v[j])) {
                subRecord = GCP.BigQueryTable.parseJSONLine(
                    Object.keys(v[j]),
                    v[j],
                    autoSchemaDetection
                );
                record[col].push(subRecord.record);
                if(autoSchemaDetection) schema[col].fields = _.deepExtend(schema[col].fields, subRecord.schema);
              }
            }
          }
          else{
            for (j=0; j<v.length; j++){
              record[col].push(GCP.BigQueryTable.value(v[j]));
            }
          }

          if(_.isEmpty(record[col])){
            record = _.omit(record, col);
            if(autoSchemaDetection) schema = _.omit(schema, col);
          }
        }
        else if(colType === 'RECORD'){
          if(!_.isEmpty(v)) {
            subRecord = GCP.BigQueryTable.parseJSONLine(Object.keys(v), v, autoSchemaDetection);
            record[col] = subRecord.record;
            if(autoSchemaDetection) schema[col].fields = subRecord.schema;
          }
        }
        else{
          record[col] = GCP.BigQueryTable.value(v);
        }
      }
    }

    return {
      record: record,
      schema: schema
    };
  },

  convertSchemaFields: function (schema, fields) {
    var keys = Object.keys(schema);
    keys.sort(function(a, b) {
      if (a === 'id') {
        return -1;
      } else if (b === 'id') {
        return 1;
      } else {
        return a.localeCompare(b);
      }
    });
    keys.forEach(function (key) {
      if(schema[key].fields){
        var val = _.clone(schema[key]);
        val.fields = [];
        GCP.BigQueryTable.convertSchemaFields(schema[key].fields, val.fields);
        fields.push(val);
      }
      else
        fields.push(schema[key]);
    });
  }
};
