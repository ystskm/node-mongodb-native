var 
      mongo = require('./lib/mongodb')
    , assert = require('assert');

function main(){
    var
      server = new mongo.Server('localhost', '27017', {poolSize:1})
    , db = new mongo.Db('repro-findAndModify', server);

    db.open(function(err) {
        if (err) throw err;

        var tracker = mkTracker(db, 2)
        startTest(db, upsertTwice, tracker);
        startTest(db, upsertInsert, tracker);
    });  
}

function mkTracker(db, expected) {

    var die = setTimeout(function() {
        console.log('tests timed out');
        process.exit(1);
    }, 5000);

    var actual = 0;
    var tracker = function (){
        actual +=1;

        if(actual === expected){
            console.log('done!')
            clearTimeout(die);
            db.close();
        }
    }    

    return tracker;
}


function startTest(db, fn, tracker) {

    process.nextTick(function(){
        console.log('test ' + fn.name);
        openAndClearCollection(db, fn.name, function(coll){
           fn(coll, tracker);
        });
    });    
}


function openAndClearCollection(db, name, callback) {

    db.collection(name, function(err, coll) {
       
       if (err) return callback(err);   
       coll.remove( {}, {safe:true}, function (err){
            if (err) throw err;
            callback(coll)
       });
    });
}

function upsertTwice(coll, callback) {
    coll.ensureIndex( {un: 1}, {unique: true}, function (err) {
        if (err) throw err;

        var   key = { un: 1}
            , rec = {
                un: 1,
                other: 'foo'
            }
            , opts = { upsert: true, 'new' : true, safe:true }
           ;


        coll.findAndModify( key, [], rec, opts, function (err, res) {
            if (err) throw err;

            var id = res._id;
            assert.ok(id);
            delete rec._id;

            coll.findAndModify(key,[], rec, opts, function (err, res) {
               if (err) throw err;
               assert.deepEqual(id, res._id);
               callback(); 
            });
            
        })
      
    });
}

function upsertInsert(coll, callback) {
    coll.ensureIndex( {un: 1}, {unique: true}, function (err) {
        if (err) throw err;

        var   key = { un: 2 }
            , rec = {
                un: key.un,
                other: 'foo'
            }
            , opts = { upsert: true, 'new' : true, safe:true }
            ;


        coll.findAndModify( key, [], rec, opts, function (err, res) {
            if (err) throw err;

            var id = res._id;
            assert.ok(id);
            delete rec._id;

            coll.insert( rec, { safe: true},  function (err, res){
                assert.ok(err);
                assert.ok(err.message.match(/duplicate/));
                callback(); 
            });
            
        })
      
    });
}

main()