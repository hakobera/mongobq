var assert = require('assert');
var GCP = require('../lib/gcp');
var moment = require('moment');
var _ = require('underscore');
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

      it ('should return array of objects when value is array that include object', function () {
        var ary = [{key: 'val1'}, {key: 'val2'}];
        var ret = GCP.BigQueryTable.value(ary);

        ary.forEach(function (item, i) {
          assert.equal(ret[i], Object(ary[i]));
        });
      });

      it ('should return object when value is object', function () {
        var obj = { key: 'val' };
        assert.equal(GCP.BigQueryTable.value(obj), Object(obj));
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

      it ('should return RECORD when value is object', function () {
        var obj = { key: 'val' };
        assert.equal(GCP.BigQueryTable.type(obj), 'RECORD');
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

      it ('should also include fields if val type is RECORD', function() {
        var key = 'abc';
        var val = {a: 123, b: "bbb", c: new Date()};
        var ret = GCP.BigQueryTable.column(key, val);

        assert.equal(ret.name, 'abc');
        assert.equal(ret.type, 'RECORD');
        assert.equal(ret.mode, 'NULLABLE');
        assert.equal(ret.fields, Object(ret.fields));
      });

      it ('should be able to detect array value', function (){
        var key = 'a';
        var val = [{'one': 1, 'two': 2}, {'one': 11, 'two': 22}];
        var ret = GCP.BigQueryTable.column(key, val);

        assert.equal(ret.name, 'a');
        assert.equal(ret.type, 'RECORD');
        assert.equal(ret.mode, 'REPEATED');
        assert.equal(ret.fields, Object(ret.fields));
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
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": {
              y: {
                "name": "y",
                "type": "DATE",
                "mode": "NULLABLE"
              },
              z: {
                "name": "z",
                "type": "STRING",
                "mode": "NULLABLE"
              },
              x: {
                "name": "x",
                "type": "RECORD",
                "mode": "REQUIRED",
                "fields": {
                  zz: {
                    "name": "zz",
                    "type": "BOOLEAN",
                    "mode": "NULLABLE"
                  },
                  id: {
                    "name": "id",
                    "type": "INTEGER",
                    "mode": "REQUIRED"
                  },
                  aa: {
                    "name": "aa",
                    "type": "STRING",
                    "mode": "REPEATED"
                  }
                }
              }
            }
          },

          id: {
            "name": "id",
            "type": "STRING",
            "mode": "REQUIRED"
          },

          def: {
            "name": "def",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": {
              a: {
                "name": "a",
                "type": "INTEGER",
                "mode": "NULLABLE"
              },
              b: {
                "name": "b",
                "type": "FLOAT",
                "mode": "REQUIRED"
              },
              c: {
                "name": "c",
                "type": "STRING",
                "mode": "REQUIRED"
              }
            }
          }
        };

        GCP.BigQueryTable.convertSchemaFields(schema, fields);

        assert.equal(fields.length, 3);
        assert.equal(fields[0].name, 'id');
        assert.equal(fields[1].name, 'abc');
        assert.equal(fields[2].name, 'def');
        assert.equal(fields[1].fields.length, 3);
        assert.equal(fields[1].fields[0].name, 'x');
        assert.equal(fields[1].fields[0].fields.length, 3);
        assert.equal(fields[1].fields[0].fields[0].name, 'id');
        assert.equal(fields[1].fields[0].fields[1].name, 'aa');
        assert.equal(fields[1].fields[0].fields[2].name, 'zz');
        assert.equal(fields[1].fields[1].name, 'y');
        assert.equal(fields[1].fields[2].name, 'z');
        assert.equal(fields[2].fields.length, 3);
        assert.equal(fields[2].fields[0].name, 'a');
        assert.equal(fields[2].fields[1].name, 'b');
        assert.equal(fields[2].fields[2].name, 'c');
      });
    });

    describe ('#parseJSONLines', function(){
      it ('should parse recursive JSON objects', function() {
        var data = {
          "_id" : ObjectID("5553566950c403ef04f55d76"),
          "player" : {
            "status" : "active",
            "email" : "test@email.com",
            "photoURL" : "https://google.com",
            "name" : "Just a test name",
            "_id" : ObjectID("5551c5ee7a19fd8a05ebf6cf")
          },
          "app" : {
            "company" : ObjectID("00000000000000000000000a"),
            "facebookAppId" : "1234567890",
            "num" : 1,
            "status" : "active",
            "countries": [],
            "_id" : ObjectID("55487a238a5458da3701669e")
          },
          "created" : {
            "at" : moment("2015-05-13T13:49:29.907+0000").toDate(),
            "ref" : "Player",
            "user" : ObjectID("5551c5ee7a19fd8a05ebf6cf")
          },
          "rewardIds" : [
              12345,
              67890,
              -4444
          ],
          "localPrice" : 100,
          "survey" : {
            "questions" : [
              "Your mobile number",
              "What is this?",
              "Have you try this?"
            ],
            "answers" : [
              {
                "answer" : [
                  "08571234567"
                ]
              },
              {
                "answer" : [
                  "Reward Voucher"
                ]
              },
              {
                "answer" : [
                  "Nope"
                ]
              }
            ],
            "_id" : ObjectID("554c9e240e6899890a3d0d28")
          },
          "code" : "REWARDCODE",
          "shippingAddress" : null,
          "status" : "claimed",
          "__v" : 0
        };

        var ret = GCP.BigQueryTable.parseJSONLine(Object.keys(data), data, true);

        assert.equal(ret.record, Object(ret.record));
        assert.equal(ret.schema, Object(ret.schema));
        assert.equal(ret.record.id, "5553566950c403ef04f55d76");
        assert.equal(ret.schema.id.type, "STRING");
        assert.equal(ret.record.app.countries, null);
        assert.equal(ret.schema.app.fields.countries, null);
        assert.equal(ret.record.created.at, "2015-05-13 13:49:29");
        assert.equal(ret.schema.created.fields.at.type, "TIMESTAMP");
        assert.equal(ret.record.rewardids[0], 12345);
        assert.equal(ret.schema.rewardids.mode, "REPEATED");
        assert.equal(ret.record.survey.answers.length, 3);
        assert.equal(ret.record.survey.answers[0].answer[0], "08571234567");
        assert.equal(ret.schema.survey.fields.answers.fields.answer.type, "STRING");
        assert.equal(ret.record.__v, null);
        assert.equal(ret.schema.__v, null);
      })
    });
  });
});
