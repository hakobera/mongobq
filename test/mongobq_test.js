var assert = require('assert');
var assert = require('assert');
var exec = require('child_process').exec;
var mkdirp = require('mkdirp');
var path = require('path');
var rimraf = require('rimraf');
var spawn = require('child_process').spawn;

var binPath = path.resolve(__dirname, '../bin/mongobq');
var tempDir = path.resolve(__dirname, '../temp');

describe('-h', function () {
   var dir;

  before(function (done) {
    createEnvironment(function (err, newDir) {
      if (err) return done(err);
      dir = newDir;
      done();
    });
  });

  after(function (done) {
    this.timeout(30000);
    cleanup(dir, done);
  });

  it('should print usage', function (done) {
    run(dir, ['-h'], function (err, stdout) {
      if (err) return done(err);

      assert.ok(/Usage: mongobq/.test(stdout));
      assert.ok(/--help/.test(stdout));
      assert.ok(/--version/.test(stdout));
      done();
    });
  });
});

function cleanup(dir, callback) {
  if (typeof dir === 'function') {
    callback = dir;
    dir = tempDir;
  }

  rimraf(tempDir, function (err) {
    callback(err);
  });
}

function createEnvironment(callback) {
  var num = process.pid + Math.random();
  var dir = path.join(tempDir, ('app-' + num));

  mkdirp(dir, function ondir(err) {
    if (err) return callback(err);
    callback(null, dir);
  });
}

function run(dir, args, callback) {
  var argv = [binPath].concat(args);
  var chunks = [];
  var exec = process.argv[0];
  var stderr = [];

  var child = spawn(exec, argv, {
    cwd: dir
  });

  child.stdout.on('data', function ondata(chunk) {
    chunks.push(chunk);
  });
  child.stderr.on('data', function ondata(chunk) {
    stderr.push(chunk);
  });

  child.on('error', callback);
  child.on('exit', function onexit() {
    var err = null;
    var stdout = Buffer.concat(chunks)
      .toString('utf8')
      .replace(/\x1b\[(\d+)m/g, '_color_$1_');

    try {
      assert.equal(Buffer.concat(stderr).toString('utf8'), '');
    } catch (e) {
      err = e;
    }

    callback(err, stdout);
  });
}
