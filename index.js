var fs = require('fs')
  , path = require('path')
  , util = require('util')
  , clone = require('clone')
  , Readable = require("stream").Readable
  , PassThrough = require('stream').PassThrough
  , binaryCSV = require("binary-csv")
  , Streamifier = require('./lib/util').Streamifier
  , Ldjsonifier = require('./lib//util').Ldjsonifier
  , Filter = require('./lib//util').Filter
  , request = require('request')
  , Validator = require('jts-validator')
  , semver = require('semver')
  , mime = require('mime')
  , split = require('split');

module.exports = Dpkg;

function Dpkg(dpkg, root, options){
  options = options || {};
  
  this.dpkg = dpkg;
  this.root = (root) ? path.resolve(root): process.cwd();
  this.registry = options.registry || 'http://registry.standardanalytics.io';
};

Dpkg.prototype.get = function(name){
  return this.dpkg.resources.filter(function(x){return x.name === name})[0];
};

Dpkg.prototype._fail = function(msg){
  var s = new Readable(this.options);   
  process.nextTick(function(){
    stream.emit('error', new Error(msg));
  });
  return s;  
};

Dpkg.prototype._url = function(require){
  return [this.registry, require.datapackage, semver.clean(this.dpkg.dataDependencies[require.datapackage]), require.resource].join('/');
};

Dpkg.prototype.createReadStream = function(name, options){
  options = clone(options) || {};
  if(options.coerce || options.ldjsonify){
    options.objectMode = true;
  }

  var r = this.get(name);
  if(!r) return this._fail('resource '+ name + ' does not exist');

  var s; //the stream we will return

  //first get a raw stream of the resource
  var isRemote = false;
  
  //order matters
  if('data' in r){
    s =  new Streamifier(r.data, options);
  } else if('url' in r){
    s = new PassThrough(options);
    isRemote = true;
  } else if('path' in r){
    s = fs.createReadStream(path.resolve(this.root, r.path));
  } else if('require' in r){
    s = new PassThrough(options);
    isRemote = true;
  } else {
    return this._fail('could not find "data", "url", "path" or "require"');
  }

  if(isRemote){
    //return immediately the empty PassThrough stream that will be fed by data when the request is resolved

    var feed = function (format, schema){
      request(r.url || this._url(r.require))
        .on('response', function(resp){
          format = format || mime.extension(resp.headers['content-type']);
          this._convert(resp, options, format, schema).pipe(s);
        }.bind(this))
        .on('error', function(err){
          s.emit('error', err);
        });
    }.bind(this);
    
    if( ('require' in r) && !('url' in r) ){
      //on the registry, we get the resource without data to get a schema if it exists
      request(this._url(r.require) + '?meta=true', function(err, body, resp){
        if(err) return s.emit('error', err);
        if(resp.statusCode === 200){
          body = JSON.parse(body);
          feed(body.format, body.schema);
        }       
      }.bind(this));
    } else {
      feed(r.schema);
    }
    return s;

  }else{
    return this._convert(s, options, r.format, r.schema);
  }
};


/**
 * s is a raw stream with a special properties format and schena attached to it
 */  
Dpkg.prototype._convert = function(s, options, format, schema){
  if(!options.objectMode){
    return s;
  }

  //Parsing: binary stream -> stream in objectMode
  if(format === 'csv'){
    s = s.pipe(binaryCSV({json:true}));

  } else if (format === 'ldjson'){ //line delimited JSON
    s = s.pipe(split(function(row){
      if(row) {
        return JSON.parse(row);
      }
    }));
  }

  //coercion and transformation  
  if(options.coerce && schema){
    s = s.pipe(new Validator(schema));
  }

  if(options.ldjsonify){
    s = s.pipe(new Ldjsonifier());
  }

  return s;
};

