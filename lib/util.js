var util = require("util")
  , clone = require('clone')
  , Readable = require("stream").Readable
  , Transform = require("stream").Transform;


function Filter(keys, options) {
  options = clone(options) || {};
  options.objectMode = true;

  this._keys = keys.slice();

  Transform.call(this, options);
};

util.inherits(Filter, Transform);

Filter.prototype._transform = function(chunk, encoding, done){

  var obj = {};
  this._keys.forEach(function(key){
    if(key in chunk){
      obj[key] = chunk[key];
    }
  });

  this.push(obj);
  
  done();

};


function Ldjsonifier(options) {
  options = clone(options) || {};
  Transform.call(this, options);
  this._writableState.objectMode = true;
  this._readableState.objectMode = false;
};

util.inherits(Ldjsonifier, Transform);

Ldjsonifier.prototype._transform = function(chunk, encoding, done){
  this.push(JSON.stringify(chunk)+ '\n');  
  done();
};



function Streamifier(data, options){
  options = options || {};

  Readable.call(this, options);

  this._data = new Buffer((typeof data === 'string') ? data : JSON.stringify(data));
};

util.inherits(Streamifier, Readable);

Streamifier.prototype._read = function (size){
  this.push(this._data);
  this.push(null);
};

exports.Filter = Filter;
exports.Streamifier = Streamifier;
exports.Ldjsonifier = Ldjsonifier;