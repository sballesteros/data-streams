var fs = require('fs')
  , path = require('path')
  , util = require('util')
  , clone = require('clone')
  , Readable = require("stream").Readable
  , PassThrough = require('stream').PassThrough
  , binaryCSV = require("binary-csv")
  , Streamifier = require('./lib/util').Streamifier
  , Ldjsonifier = require('./lib/util').Ldjsonifier
  , Filter = require('./lib//util').Filter
  , request = require('request')
  , Validator = require('jts-validator')
  , semver = require('semver')
  , mime = require('mime')
  , isUrl = require('is-url')
  , url = require('url')
  , split = require('split');

exports.Ctnr = Ctnr;
exports.CtnrSync = CtnrSync;

function CtnrSync(root, opts){  
  var ctnr = JSON.parse(fs.readFileSync(path.resolve(root, 'container.jsonld')));

  Ctnr.call(this, ctnr, root, opts);  
};
util.inherits(CtnrSync, Ctnr);


function Ctnr(ctnr, root, opts){
  opts = opts || {};
  
  this.ctnr = ctnr;
  this.root = (root) ? path.resolve(root): process.cwd();

  //if !base in this -> will throw if based is needed. if this.based === undefined try to get a base by dereferencing context url
  if(ctnr['@context']){
    if(isUrl(ctnr['@context'])){
      this.base = undefined; //will be resolved at first request if needed...
    } else if ( (typeof ctnr['@context'] === 'object') && ('@base' in ctnr['@context']) && isUrl(ctnr['@context']['@base']) ) {
      this.base = ctnr['@context']['@base'];
    }
  } else if(opts.base && isUrl(opts.base)) {
    this.base = opts.base;
  }

};


Ctnr.prototype.get = function(name){
  return clone(this.ctnr.dataset.filter(function(x){return x.name === name})[0]);
};


Ctnr.prototype.createReadStream = function(name, opts){
  opts = clone(opts) || {};
  if(opts.coerce || opts.ldjsonify){
    opts.objectMode = true;
  }

  var r = this.get(name);
  if(!r) return _fail('dataset ' + name + ' does not exist');
  if(!r.distribution) return _fail('dataset: ' + name + ' has no distribution');
  
  var s; //the stream that will be returned

  //first get a raw stream of the resource
  var isRemote = false;
  
  //order matters
  if('contentData' in r.distribution){
    s =  new Streamifier(r.distribution.contentData);
  } else if('contentPath' in r.distribution){ //local first
    s = fs.createReadStream(path.resolve(this.root, r.distribution.contentPath));
    //TODO if no encodingFormat try chance by using contentUrl if it exists
  } else if('contentUrl' in r.distribution){
    s = new PassThrough(opts);
    isRemote = true;
  } else {
    return _fail('dataset: ' + name + ' could not find "contentData", "contentPath" or "contentUrl" in distribution');
  }

  if (isRemote){
    //return immediately the empty PassThrough stream that will be fed by data when the request is resolved
    
    //if not format or schema but a hope that's it's on the registry: try to get format or schema from the registry
    if( !isUrl(r.distribution.contentUrl) ){ 

      //try to get base
      if(this.base){

        this._feed(s, r, opts);        

      } else if ('base' in this){ //try to retrieve @context.@base
        
        request(this.ctnr['@context'], function(err, resp, body){
          if(err) return s.emit('error', err);
          if(resp.statusCode >= 400){
            s.emit('error', new Error('could not retrieve @context at: ' + this.ctnr['@context'] + '. Server returned error code: '+  resp.statusCode));
            return;
          }
          
          try{
            var ctx = JSON.parse(body);
          } catch (e){
            return s.emit('error', e);
          }

          if ( ctx['@base'] && isUrl(ctx['@base']) ){
            this.base = ctx['@base'];
            this._feed(s, r, opts);            
          } else {
            s.emit('error', new Error('@context retrieved at: ' + this.ctnr['@context'] + ' does not contain a valid @base'));            
          }
          
        }.bind(this));

      } else {

        s.emit('error', new Error('dataset: ' + name + ' has a relative url and no base could be found'));

      }

    } else {
      this._feed(s, r, opts);
    }

    return s;

  } else {

    return this._convert(s, r, opts);

  }
};


/**
 * feed passthrough stream s with remote resource r
 */ 
Ctnr.prototype._feed = function (s, r, opts){

  var rurl = (isUrl(r.distribution.contentUrl)) ? r.distribution.contentUrl : url.resolve(this.base, r.distribution.contentUrl);

  request(rurl)
    .on('response', function(resp){
      r.distribution.encodingFormat = r.distribution.encodingFormat || resp.headers['content-type']; //last hope to get an encodingFormat
      if(resp.statusCode >= 400){
        s.emit('error', new Error(resp.statusCode));
      } else {
        this._convert(resp, r, opts).pipe(s);        
      }
    }.bind(this))
    .on('error', function(err){
      s.emit('error', err);
    });

};



/**
 * convert s, a raw stream (Buffer) according to the format of r and
 * opts
 */  
Ctnr.prototype._convert = function(s, r, opts){

  if(!opts.objectMode){
    return s;
  }

  var contentType = r.distribution.encodingFormat;

  if(!contentType){
    //TODO allow force opts to force an encodingFormat 
    process.nextTick(function(){
      s.emit('error', new Error('no encodingFormat could be found for ' + r.name));
    });
    return s;
  }
  
  if( (contentType !== 'text/csv') && (contentType !== 'application/x-ldjson') ){
    process.nextTick(function(){
      s.emit('error', new Error('opts ' + Object.keys(opts).join(',') +' can only be specified for resource of format csv or ldjson, not ' + contentType + ' ( resource: ' + r.name +')'));
    });
    return s;    
  }

  //Parsing: binary stream -> stream in objectMode
  if(contentType === 'text/csv'){
    s = s.pipe(binaryCSV({json:true}));

  } else if (contentType === 'application/x-ldjson'){ //line delimited JSON
    s = s.pipe(split(function(row){
      if(row) {
        return JSON.parse(row);
      }
    }));
  }
  
  //coercion and transformation  
  if(r.about && opts.coerce){
    s = s.pipe(new Validator(r.about));
  }

  if(opts.filter){
    s = s.pipe(new Filter(opts.filter, opts));
  }
  
  if(opts.ldjsonify){
    if(contentType !== 'text/csv'){
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
