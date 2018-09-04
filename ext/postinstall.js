/***/
(function() {
  var NULL = null, TRUE = true, FALSE = false;
  var fs = require('fs');
  Promise.resolve().then(()=>{
  
    // Overwrite the installed script in node_modules by the good script
    return Promise.all([ 
      copyFile('lib/bson/bson.js' , 'node_modules/bson/lib/bson/bson.js'),
      copyFile('lib/bson/index.js', 'node_modules/bson/ext/index.js')
    ]);
  
  }).then(()=>{
    console.log('PostInstall process successfully!');
  })['catch'](e=>{
    console.log('PostInstall process failed ... ', e);
    throw e;
  });
  function copyFile(src, dst) {
    return Promise.resolve().then(()=>{
      return new Promise((rsl, rej)=>{
        
        fs.createReadStream(src).pipe( fs.createWriteStream(dst, {
          flags: 'w',
          autoClose: FALSE
        }).on('close', rsl) );
        
      });
    }).then(()=>{
      console.log('Copy ' + src + ' => ' + dst + ' finished!');
    });
  }
})();