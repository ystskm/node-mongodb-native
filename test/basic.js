var nodeunit = require('nodeunit');

var mongodb = require('../index');
var Db = mongodb.Db, Server = mongodb.Server;

var svr, dao, col;
module.exports = nodeunit.testCase({
  'connect': function(t) {
    svr = new Server('127.0.0.1', '27017', {
      poolSize: 50
    });
    dao = new Db('test', svr, {
      safe: true,
      override_used_flag: true
    });
    dao.open(function(e, obj) {
      t.equal(e, null, 'openE');
      t.deepEqual(obj, dao, 'openR');
      dao.collection('test', function(e, obj) {
        t.equal(e, null, 'collectionE');
        col = obj;
        t.done();
      });
    });
  },
  'init': function(t) {
    col.count(function(e, r) {
      t.equal(e, null, 'countE');
      t.equal(typeof r, 'number', 'countR');
      col.remove({}, function(e, r) {
        t.equal(e, null, 'removeE');
        t.equal(typeof r, 'number', 'removeR');
        t.done();
      });
    });
  },
  'CLUD': function(t) {
//    Promise;
    col.insert([{a: 1}, {a:2}], function(e, r){
      t.equal(e, null, 'insertE');
      t.equal(Array.isArray(r), true);
      t.equal(r.length, 2);
      t.equal(r[0].a, 1, 'insertR');
      col.count({}, function(e, r){
        t.equal(e, null, 'countE');
        t.equal(r, 2, 'countR');
        col.update({a:1}, {$set: {b:1} }, function(e, r){
          t.equal(e, null, 'updateE');
          t.equal(r, 1, 'updateR');
          col.findOne({a: 1}, function(e, r){
            t.equal(e, null, 'findOneE');
            t.equal(r.a, 1, 'findOneRa');
            t.equal(r.b, 1, 'findOneRb');
            col.remove({}, function(e, r){
              t.equal(e, null, 'removeE');
              t.equal(r, 2, 'removeR');
              t.done();
            });
          });
        });
      });
    });
  },
  'finalize': function(t){
    svr.close();
    t.done();
  }
});
