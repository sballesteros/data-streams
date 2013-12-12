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
    s =  new Streamifier(r.data, r.format, options);
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
    return this._fail('could not find "data", "url", "path" or "require"');
  }

  if(isRemote){
    //return immediately the empty PassThrough stream that will be fed by data when the request is resolved

    var feed = function (format, schema, fields){
      request(r.url || this._url(r.require))
        .on('response', function(resp){
          format = format || mime.extension(resp.headers['content-type']);
          if(resp.statusCode === 200){
            this._convert(resp, name,  options, format, schema, fields).pipe(s);
          } else {
            s.emit('error', new Error(resp.statusCode));          
          }    
        }.bind(this))
        .on('error', function(err){
          s.emit('error', err);
        });
    }.bind(this);
    
    if( ('require' in r) && !('url' in r) ){
      //on the registry, we get the resource without data to get a schema if it exists
      request(this._url(r.require) + '?meta=true', function(err, resp, body){
        if(err) return s.emit('error', err);
        if(resp.statusCode === 200){
          body = JSON.parse(body);
          feed(body.format, body.schema, r.require.fields);
        } else {
          s.emit('error', new Error(resp.statusCode));          
        }    
      }.bind(this));
    } else {
      feed(r.format, r.schema, r.fields);
    }
    return s;

  }else{
    return this._convert(s, name, options, r.format, r.schema, r.fields);
  }
};


/**
 * s is a raw stream
 */  
Dpkg.prototype._convert = function(s, name, options, format, schema, fields){
  if(!options.objectMode){
    return s;
  }

  //TODO check format...
  if(!format){
    s.emit('error', new Error('no format could be specified for ' + name));
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
  } else if (format === 'json') {
    return s;
  }
  
  //coercion and transformation  
  if(schema && options.coerce){
    s = s.pipe(new Validator(schema));
  }

  if(fields){
    s = s.pipe(new Filter(fields, options));
  }
  
  if(options.ldjsonify){
    if(format !== 'csv'){
      s.emit('error', new Error('ldjsonify can only be used with csv data'))
      return s;
    }
    s = s.pipe(new Ldjsonifier());
  }

  return s;
};
