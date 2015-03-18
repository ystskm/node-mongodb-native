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
      console.log('init.count: ', arguments);
      col.remove({}, function(e, r) {
        t.equal(e, null, 'removeE');
        t.equal(typeof r, 'number', 'removeR');
        console.log('init.remove: ', arguments);
        t.done();
      });
    });
  },
  'CLUD': function(t) {
    //    Promise;
    col.insert([{
      a: 1
    }, {
      b: 2
    }], function(e, r) {
      t.equal(e, null, 'insertE');
      t.equal(Array.isArray(r), true);
      t.equal(r.length, 2);
      t.equal(r[0].a, 1, 'insertR');
      console.log('CLUD.insert: ', arguments);
      col.count({}, function(e, r) {
        t.equal(e, null, 'countE');
        t.equal(r, 2, 'countR');
        console.log('CLUD.count: ', arguments);
        col.update({
          a: 1
        }, {
          $set: {
            b: 1
          }
        }, function(e, r) {
          t.equal(e, null, 'updateE');
          t.equal(r, 1, 'updateR');
          console.log('CLUD.update: ', arguments);
          col.findOne({
            a: 1
          }, function(e, r) {
            t.equal(e, null, 'findOneE');
            t.equal(r.a, 1, 'findOneRa');
            t.equal(r.b, 1, 'findOneRb');
            console.log('CLUD.findOne: ', arguments);
            col.remove({}, function(e, r) {
              t.equal(e, null, 'removeE');
              t.equal(r, 2, 'removeR');
              console.log('CLUD.remove: ', arguments);
              t.done();
            });
          });
        });
      });
    });
  },
  'finalize': function(t) {
    svr.close();
    t.done();
  }
});
