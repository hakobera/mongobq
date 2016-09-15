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
      assert.equal(GCP.safeName('az:19:AZ'), 'az_19_az');
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
        assert.equal(GCP.BigQueryTable.value('abc'), 'abc');
      });

      it ('should return original value when value is number', function () {
        assert.equal(GCP.BigQueryTable.value(1.0), 1.0);
      });

      it ('should return original value when value is boolean', function () {
        assert.equal(GCP.BigQueryTable.value(true), true);
        assert.equal(GCP.BigQueryTable.value(false), false);
      });

      it ('should return formated string when value is Date', function () {
        var date = new Date(Date.parse('2014-12-21T01:02:03'));
        assert.equal(GCP.BigQueryTable.value(date), '2014-12-21 01:02:03');
      });

      it ('should return string when value is mongodb.ObjectID', function () {
        var id = new ObjectID();
        assert.equal(GCP.BigQueryTable.value(id), id.toString());
      });

      it ('should return array when value is array', function () {
        var ary = ['a', 'b', 'c'];
        var ret = GCP.BigQueryTable.value(ary);

        ary.forEach(function (item, i) {
          assert.equal(ret[i], ary[i]);
        });
      });

      it ('should return array when value is array that include object', function () {
        var ary = [{key: 'val1'}, {key: 'val2'}];
        var ret = GCP.BigQueryTable.value(ary);

        ary.forEach(function (item, i) {
          assert.equal(ret[i], JSON.stringify(ary[i]));
        });
      });

      it ('should return JSON string when value is object', function () {
        var obj = { key: 'val' };
        assert.equal(GCP.BigQueryTable.value(obj), JSON.stringify(obj));
      });
    });


    describe('#type', function () {
      it ('should return STRING value when value is string', function () {
        assert.equal(GCP.BigQueryTable.type('abc'), 'STRING');
      });

      it ('should return FLOAT when value is float', function () {
        assert.equal(GCP.BigQueryTable.type(1), 'FLOAT');
        assert.equal(GCP.BigQueryTable.type(1.0), 'FLOAT');
      });

      it ('should return BOOLEAN when value is boolean', function () {
        assert.equal(GCP.BigQueryTable.type(true), 'BOOLEAN');
        assert.equal(GCP.BigQueryTable.type(false), 'BOOLEAN');
      });

      it ('should return TIMESTAMP when value is Date', function () {
        var date = new Date(Date.parse('2014-12-21T01:02:03'));
        assert.equal(GCP.BigQueryTable.type(date), 'TIMESTAMP');
      });

      it ('should return STRING when value is mongodb.ObjectID', function () {
        var id = new ObjectID();
        assert.equal(GCP.BigQueryTable.type(id), 'STRING');
      });

      it ('should return STRING when value is array', function () {
        var ary = ['a', 'b', 'c'];
        assert.equal(GCP.BigQueryTable.type(ary), 'STRING');
      });

      it ('should return STRING when value is object', function () {
        var obj = { key: 'val' };
        assert.equal(GCP.BigQueryTable.type(obj), 'STRING');
      });
    });

    describe('#mode', function () {
      it ('should return REPEATED when value is array', function () {
        assert.equal(GCP.BigQueryTable.mode([]), 'REPEATED');
      });

      it ('should return NULLABLE when value is not array', function () {
        assert.equal(GCP.BigQueryTable.mode('a'), 'NULLABLE');
      });
    });

    describe('#column', function () {
      it ('should return correct column definition', function () {
        var key = 'key:1';
        var val = 'val';
        var ret = GCP.BigQueryTable.column(key, val);

        assert.equal(ret.name, 'key_1');
        assert.equal(ret.type, 'STRING');
        assert.equal(ret.mode, 'NULLABLE');
      });
    });

    describe('#convertSchemaFields', function () {
      var fields;

      beforeEach(function () {
        fields = [];
      });

      it ('should convert has to array that sorted by key as alphabet order, but special fields "id" is always first column', function () {
        var schema = {
          abc: {
            "name": "abc",
            "type": "STRING"
          },

          id: {
            "name": "id",
            "type": "STRING"
          },

          def: {
            "name": "def",
            "type": "STRING"
          }
        };

        GCP.BigQueryTable.convertSchemaFields(schema, fields);

        assert.equal(fields.length, 3);
        assert.equal(fields[0].name, 'id');
        assert.equal(fields[1].name, 'abc');
        assert.equal(fields[2].name, 'def');
      });
    });
  });
});
