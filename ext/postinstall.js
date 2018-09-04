/***/
(function() {
  
  var NULL = null, TRUE = true, FALSE = false;
  var fs = require('fs');
  
  // Overwrite the installed script in node_modules by the good script
  fs.copyFileSync('lib/bson/bson.js', 'node_modules/bson/lib/bson/bson.js');
  fs.copyFileSync('lib/bson/index.js',  'node_modules/bson/ext/index.js');
  
})();