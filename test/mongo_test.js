var assert = require('assert');
var Mongo = require('../lib/mongo');

describe('Mongo', function () {
  var mongo;

  describe('#mongodbUri', function () {
    describe('without username and password', function () {
      before(function () {
        mongo = new Mongo({
          host: 'localhost',
          port: 27017,
          database: 'mydb'
        });
      });

      it('should return uri for mongodb', function () {
        assert.equal(mongo.mongodbUri, 'mongodb://localhost:27017/mydb');
      });
    });

    describe('with username and password', function () {
      before(function () {
        mongo = new Mongo({
          host: 'localhost',
          port: 27017,
          database: 'mydb',
          username: 'user',
          password: 'pass'
        });
      });

      it('should return uri for mongodb', function () {
        assert.equal(mongo.mongodbUri, 'mongodb://user:pass@localhost:27017/mydb');
      });
    });
  });
});
