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

exports.Dpkg = Dpkg;
exports.DpkgSync = DpkgSync;

function DpkgSync(root, options){  
  var dpkg = JSON.parse(fs.readFileSync(path.resolve(root, 'package.json')));

  Dpkg.call(this, dpkg, root, options);  
};
util.inherits(DpkgSync, Dpkg);


function Dpkg(dpkg, root, options){
  options = options || {};
  
  this.dpkg = dpkg;
  this.root = (root) ? path.resolve(root): process.cwd();
  this.registry = options.registry || 'http://registry.standardanalytics.io';
};


Dpkg.prototype.get = function(name){
  return this.dpkg.resources.filter(function(x){return x.name === name})[0];
};


Dpkg.prototype._url = function(require, callback){

  request(this.registry + '/' + require.datapackage, function(err, res, versions){
    if(err) return callback(err);
    
    if (res.statusCode >= 400){
      var err = new Error('fail');
      err.code = res.statusCode;
      return callback(err);
    }
    
    versions = JSON.parse(versions);
    var version = semver.maxSatisfying(versions, this.dpkg.dataDependencies[require.datapackage]);
    
    callback(null, [this.registry, require.datapackage, version, require.resource].join('/'));
    
  }.bind(this));

};

Dpkg.prototype.createReadStream = function(name, options){
  options = clone(options) || {};
  if(options.coerce || options.ldjsonify){
    options.objectMode = true;
  }

  var r = this.get(name);
  if(!r) return _fail('resource '+ name + ' does not exist');

  var s; //the stream we will return

  //first get a raw stream of the resource
  var isRemote = false;
  
  //order matters
  if('data' in r){
    s =  new Streamifier(r.data);
    if(!r.format && (typeof r.data !== 'string')){
      r.format = 'json';
    }
  } else if('url' in r){
    s = new PassThrough(options);
    isRemote = true;
  } else if('path' in r){
    s = fs.createReadStream(path.resolve(this.root, r.path));
    if(!r.format){
      r.format = path.extname(r.path).substring(1);
    }
  } else if('require' in r){
    s = new PassThrough(options);
    isRemote = true;
  } else {
    return _fail('could not find "data", "url", "path" or "require"');
  }

  if(isRemote){
    //return immediately the empty PassThrough stream that will be fed by data when the request is resolved    
    
    //if not format or schema but a hope that's it's on the registry: try to get format or schema from the registry
    if( ('require' in r) && ! ('url' in r) && ( !('format' in r) || !('schema' in r) ) ){ 

      this._url(r.require, function(err, rurl){
        if(err) return s.emit('error', err);
        request(rurl + '?meta=true', function(err, resp, body){
          if(err) return s.emit('error', err);
          if(resp.statusCode === 200){
            body = JSON.parse(body);
            if(!('format' in r) && ('format' in body)){
              r.format = body.format;
            }
            if(!('schema' in r) && ('schema' in body)){
              r.schema = body.schema;
            }
          }
          this._feed(s, r, options);
        }.bind(this));

      }.bind(this));

    } else {
      this._feed(s, r, options);
    }

    return s;

  }else{

    return this._convert(s, r, options);

  }
};


/**
 * feed passthrough stream s with remote resource r
 */ 
Dpkg.prototype._feed = function (s, r, options){
  var that = this;

  if(r.url){
    next(r.url);
  } else {
    that._url(r.require, function(err, rurl){
      if(err) return s.emit('error', err);
      next(rurl);
    });
  }
  
  function next(rurl){
    request(rurl)
      .on('response', function(resp){
        r.format = r.format || mime.extension(resp.headers['content-type']); //last hope to get a format
        if(resp.statusCode === 200){
          that._convert(resp, r, options).pipe(s);
        } else {
          s.emit('error', new Error(resp.statusCode));          
        }    
      })
      .on('error', function(err){
        s.emit('error', err);
      });
  };

};



/**
 * convert s, a raw stream (Buffer) according to the format of r and
 * options
 */  
Dpkg.prototype._convert = function(s, r, options){

  if(!options.objectMode){
    return s;
  }

  if(!r.format){
    process.nextTick(function(){
      s.emit('error', new Error('no format could be found for ' + r.name));
    });
    return s;
  }

  if( (r.format !== 'csv') && (r.format !== 'ldjson') ){
    process.nextTick(function(){
      s.emit('error', new Error('options ' + Object.keys(options).join(',') +' can only be specified for resource of format csv or ldjson, not ' + r.format + ' ( resource: ' + r.name +')'));
    });
    return s;    
  }

  //Parsing: binary stream -> stream in objectMode
  if(r.format === 'csv'){
    s = s.pipe(binaryCSV({json:true}));

  } else if (r.format === 'ldjson'){ //line delimited JSON
    s = s.pipe(split(function(row){
      if(row) {
        return JSON.parse(row);
      }
    }));
  }
  
  //coercion and transformation  
  if(r.schema && options.coerce){
    s = s.pipe(new Validator(r.schema));
  }

  if(r.require && r.require.fields){
    s = s.pipe(new Filter(r.require.fields, options));
  }
  
  if(options.ldjsonify){
    if(r.format !== 'csv'){
      process.nextTick(function(){
        s.emit('error', new Error('ldjsonify can only be used with csv data'))
      });
      return s;
    }
    s = s.pipe(new Ldjsonifier());
  }

  return s;
};


function _fail(msg){
  var s = new Readable();   
  process.nextTick(function(){
    stream.emit('error', new Error(msg));
  });
  return s;  
};
