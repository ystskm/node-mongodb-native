/***/
var exports = module.exports = {
  setTimeout: wSetTimeout,
  clearTimeout: wClearTimeout,
  setInterval: setInterval, // no wrapping.
  clearInterval: wClearInterval
};
// [setTimeout]
function wSetTimeout() {
  var args = Array.prototype.slice.call(arguments);
  var _cb = args[0];

  // making care-leak callback.
  args[0] = function() {

    // "this" is "Timeout" instance.
    // see: timers.js L:209, "timer = new Timeout(after);"
    _cb.apply(this, arguments);
    wClearTimeout(this);

  };

  return setTimeout.apply(this, args);
};

// [clearTimeout]
function wClearTimeout(timer) {
  if(timer == null)
    return;

  clearTimeout.call(this, timer);

  delete timer.ontimeout;
  delete timer._idlePrev;
  delete timer._idleNext;
  delete timer._idleStart;
  delete timer._onTimeout;

  if(timer._handle) {
    delete timer._handle.owner;
    delete timer._handle.ontimeout;
  }

  delete timer._handle;
};

// [clearInterval]
// almost equals Node.js native clearInterval.
function wClearInterval(timer) {
  if(timer && timer._repeat) {
    timer._repeat = false;
    wClearTimeout(timer);
  }
};
