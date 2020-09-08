/***/
var NULL = null, TRUE = true, FALSE = false, UNDEF = undefined;
var nodeunit = require('foonyah-ci');
var env = require('./env.json');

var mongodb = require('../index');
var Db = mongodb.Db, Server = mongodb.Server;

var svr, dao, col;
module.exports = nodeunit.testCase({
  'connect': function(t) {
    svr = new Server(env.host, env.port, {
      poolSize: env.poolSize
    });
    dao = new Db(env.databaseName, svr, {
      safe: TRUE,
      override_used_flag: TRUE
    });
    dao.open(function(e, obj) {
      t.equal(e, NULL, 'openE');
      t.deepEqual(obj, dao, 'openR');
      dao.collection(env.collectionName, function(e, obj) {
        t.equal(e, NULL, 'collectionE');
        col = obj;
        t.done();
      });
    });
  },
  'init': function(t) {
    return Promise.resolve().then(r=>{
      return new Promise(( rsl, rej )=>{
        
        col.count(function(e, r) {
          t.equal(e, NULL, 'countE');
          t.equal(ty(r), 'number', 'countR');
          // console.log('init.count: ', arguments);
          rsl();
        });
        
      });
    }).then(r=>{
      return new Promise(( rsl, rej )=>{
        
        col.remove({}, function(e, r) {
          t.equal(e, NULL, 'removeE');
          t.equal(ty(r), 'number', 'removeR');
          // console.log('init.remove: ', arguments);
          rsl();
        });

      });
    }).then(r=>{
      t.done();
    })['catch'](e=>{
      t.fail(e);
    });
  },
  'CLUD': function(t) {
    return Promise.resolve().then(r=>{
      return new Promise(( rsl, rej )=>{
        
        // insert
        col.insert([{
          a: 1
        }, {
          b: 2
        }], function(e, r) {
          t.equal(e, NULL, 'insertE');
          t.equal(isArray(r), TRUE);
          t.equal(r.length, 2);
          t.equal(r[0].a, 1, 'insertR');
          // console.log('CLUD.insert: ', arguments);
          rsl();
        });
        
      });
    }).then(r=>{
      return new Promise(( rsl, rej )=>{
        
        // count
        col.count({}, function(e, r) {
          t.equal(e, NULL, 'countE');
          t.equal(r, 2, 'countR');
          // console.log('CLUD.count: ', arguments);
          rsl();
        });
        
      });
    }).then(r=>{
      return new Promise(( rsl, rej )=>{
        
        // update
        col.update({
          a: 1
        }, {
          $set: {
            b: 1
          }
        }, function(e, r) {
          t.equal(e, NULL, 'updateE');
          t.equal(r, 1, 'updateR');
          // console.log('CLUD.update: ', arguments);
          rsl();
        });
        
      });
    }).then(r=>{
      return new Promise(( rsl, rej )=>{

        // findOne
        col.findOne({
          a: 1
        }, function(e, r) {
          t.equal(e, NULL, 'findOneE');
          t.equal(r.a, 1, 'findOneRa');
          t.equal(r.b, 1, 'findOneRb');
          // console.log('CLUD.findOne: ', arguments);
          rsl();
        });
        
      });
    }).then(r=>{
      return new Promise(( rsl, rej )=>{
        
        col.remove({}, {
          justOne: TRUE
        }, function(e, r) {
          t.equal(e, NULL, 'removeE');
          t.equal(r, 1, 'removeR');
          // console.log('CLUD.remove: ', arguments);
          rsl();
        });
        
      });
    }).then(r=>{
      t.done();
    })['catch'](e=>{
      t.fail(e);
    });
  },
  'finalize': function(t) {
    svr.close();
    t.done();
  }
});

// ------------ //
function ty(x) {
  return typeof x;
} 
function is(ty, x) {
  return ty( x ) == ty;
}
function isFunction(x) {
  return is('function', x);
}
function isArray(x) {
  return Array.isArray(x);
}
