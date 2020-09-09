/***/
(function() {
  // 修正パッチを当てるスクリプト
  var NULL = null, TRUE = true, FALSE = false;
  var fs = require('fs');
  var keeper = setInterval(()=>console.log('PostInstall processing ... '), 5 * 1000);
  return Promise.resolve().then(()=>{
  
    // Overwrite the installed script in node_modules by the good script
    return Promise.all([ 
      copyFile('lib/bson/bson.js' , 'node_modules/bson/lib/bson/bson.js'),
      copyFile('lib/bson/index.js', 'node_modules/bson/ext/index.js')
    ]);
  
  }).then(()=>{
    console.log('PostInstall process successfully!');
    clearInterval(keeper);
  })['catch'](e=>{
    console.log('PostInstall process failed ... ', e);
    clearInterval(keeper);
    throw e;
  });
  function copyFile(src, dst) {
    return Promise.resolve().then(()=>{
      console.log('Copy ... ' + src + ' => ' + dst);
    }).then(()=>{
      return new Promise((rsl, rej)=>{
        
        fs.createReadStream(src).pipe( fs.createWriteStream(dst, {
          flags: 'w',
          autoClose: TRUE
        }).on('error', rej).on('close', rsl) );
        
      });
    }).then(()=>{
      console.log('Copy ' + src + ' => ' + dst + ' finished!');
    });
  }
})();