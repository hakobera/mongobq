var assert = require('assert');
var GCP = require('../lib/gcp');
var ObjectID = require('mongodb').ObjectID;

describe('GCP', function () {
  var gcp;

  before(function () {
    gcp = new GCP({
      project: 'test'
    });
  });

  describe('#safeName', function () {
    it('should replace non alphabet/number/underscore character to underscore', function () {
      assert.equal(GCP.safeName('az_19'), 'az_19');
      assert.equal(GCP.safeName('az:19'), 'az_19');
    });

    it('should remove first underscore', function () {
      assert.equal(GCP.safeName('_az_19'), 'az_19');
    });
  });

  describe('BigQueryTable', function () {
    var table;

    before(function () {
      table = gcp.bigqueryTable({
        dataset: 'logs',
        table: 'access_logs'
      });
    });

    describe('#fullName', function () {
      it ('should return full table name', function () {
        assert.equal(table.fullName(), 'test:logs.access_logs');
      });
    });

    describe('#value', function () {
      it ('should return original value when value is string', function () {
        assert.equal(table.value('abc'), 'abc');
      });

      it ('should return original value when value is number', function () {
        assert.equal(table.value(1.0), 1.0);
      });

      it ('should return original value when value is boolean', function () {
        assert.equal(table.value(true), true);
        assert.equal(table.value(false), false);
      });

      it ('should return formated string when value is Date', function () {
        var date = new Date(Date.parse('2014-12-21T01:02:03'));
        assert.equal(table.value(date), '2014-12-21 01:02:03');
      });

      it ('should return string when value is mongodb.ObjectID', function () {
        var id = new ObjectID();
        assert.equal(table.value(id), id.toString());
      });

      it ('should return array when value is array', function () {
        var ary = ['a', 'b', 'c'];
        assert.equal(table.value(ary), ary);
      });

      it ('should return JSON string when value is object', function () {
        var obj = { key: 'val' };
        assert.equal(table.value(obj), JSON.stringify(obj));
      });
    });


    describe('#type', function () {
      it ('should return STRING value when value is string', function () {
        assert.equal(table.type('abc'), 'STRING');
      });

      it ('should return INTEGER when value is integer', function () {
        //assert.equal(table.type(1), 'INTEGER');
      });

      it ('should return FLOAT when value is float', function () {
        //assert.equal(table.type(1.0), 'FLOAT');
      });

      it ('should return BOOLEAN when value is boolean', function () {
        assert.equal(table.type(true), 'BOOLEAN');
        assert.equal(table.type(false), 'BOOLEAN');
      });

      it ('should return TIMESTAMP when value is Date', function () {
        var date = new Date(Date.parse('2014-12-21T01:02:03'));
        assert.equal(table.type(date), 'TIMESTAMP');
      });

      it ('should return STRING when value is mongodb.ObjectID', function () {
        var id = new ObjectID();
        assert.equal(table.type(id), 'STRING');
      });

      it ('should return STRING when value is array', function () {
        var ary = ['a', 'b', 'c'];
        assert.equal(table.type(ary), 'STRING');
      });

      it ('should return STRING when value is object', function () {
        var obj = { key: 'val' };
        assert.equal(table.type(obj), 'STRING');
      });
    });

    describe('#mode', function () {
      it ('should return REPEATED when value is array', function () {
        assert.equal(table.mode([]), 'REPEATED');
      });

      it ('should return NULLABLE when value is not array', function () {
        assert.equal(table.mode('a'), 'NULLABLE');
      });
    });

    describe('#column', function () {
      it ('should return correct column definition', function () {
        var key = 'key:1';
        var val = 'val';
        var ret = table.column(key, val);

        assert.equal(ret.name, 'key_1');
        assert.equal(ret.type, 'STRING');
        assert.equal(ret.mode, 'NULLABLE');
      });
    });
  });
});
