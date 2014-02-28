var Ctnr = require('../').Ctnr
  , fs = require('fs')
  , path = require('path')
  , assert = require('assert');

describe('streams', function(){

  var root = path.resolve(__dirname, 'fixture'); 

  var ctnr;
  beforeEach(function(){
    var data = JSON.parse(fs.readFileSync(path.resolve(root, 'container.jsonld')));
    ctnr = new Ctnr(data, root);   
  });

  it('should return a vanilla stream of a resource with a "contentData" property', function(done){
    var s = ctnr.createReadStream('test_inline');
    s.on('error', function(err){ throw err; });
    s.on('data', function(data){
      var expected = [
        {"a": "a", "b": "a", "c": "z"},
        {"a": "x", "b": "v", "c": "z"},
        {"a": "y", "b": "v", "c": "z"}
      ];      
      assert.deepEqual(JSON.parse(data.toString()), expected);      
    });
    s.on('end', done);    
  });

  it('should return a vanilla stream of a resource with a "contentPath" property', function(done){
    var s = ctnr.createReadStream('test_path');
    var data = [];
    s.on('error', function(err){ throw err; });
    s.on('data', function(chunk){
      data.push(chunk);
    });
    s.on('end', function(){
      data = Buffer.concat(data);
      fs.readFile(path.join(root, 'data', 'data.csv'), function(err, expected){
        assert.deepEqual(data, expected);
        done()        
      });
    });
  });

  it('should return a vanilla stream of a resource with a "contentUrl" property (as Buffer)', function(done){
    var body = [];
    var s = ctnr.createReadStream('test_url');
    s.on('error', function(err){ throw err; });
    s.on('data', function(chunk){
      body.push(chunk);
    });
    s.on('end', function(){
      fs.readFile(path.resolve(root, 'test.csv'), function(err, expected){
        if(err) throw err;
        assert.deepEqual(Buffer.concat(body), expected);
        done()
      });     
    });
  });

  it('should stream a tabular resource in objectMode', function(done){

    var expected = [
      {date: '2012-08-02', a: '6.2',    b: '10',   c: '9',    d: '5'},
      {date: '2012-08-16', a: 'null',   b: 'null', c: 'null', d: 'null'},
      {date: '2012-09-20', a: '884.4',  b: '1025', c: '2355', d: '111'},
      {date: '2012-10-04', a: '3076.5', b: 'null', c: '4783', d: '148'}
    ];

    var s = ctnr.createReadStream('test_path', {objectMode:true});
    s.on('error', function(err){ throw err; });

    var counter = 0;
    s.on('data', function(data){ 
      assert.deepEqual(data, expected[counter++]); 
    });
    s.on('end', done);
  });


  it('should coerce values', function(done){

    function isoify (x){
      x = x.split('-');
      x = new Date(Date.UTC(x[0], x[1]-1, x[2], 0, 0, 0, 0));
      return x.toISOString();
    }

    var expected = [
      {date: isoify('2012-08-02'), a: 6.2,    b: 10,   c: 9,    d: 5},
      {date: isoify('2012-08-16'), a: null,   b: null, c: null, d: null},
      {date: isoify('2012-09-20'), a: 884.4,  b: 1025, c: 2355, d: 111},
      {date: isoify('2012-10-04'), a: 3076.5, b: null, c: 4783, d: 148}
    ];

    var s = ctnr.createReadStream('test_path', {coerce:true});
    s.on('error', function(err){ throw err; });

    var counter = 0;
    s.on('data', function(obj){
      obj.date = obj.date.toISOString();
      for(var key in obj){
        assert.strictEqual(obj[key], expected[counter][key]);          
      }
      counter++;
    });

    s.on('end', done);       
  });

  it('should filter', function(done){
    
    var expected = [
      {a: 6.2,    c: 9 },
      {a: null,   c: null },
      {a: 884.4,  c: 2355 },
      {a: 3076.5, c: 4783 }
    ];

    var s = ctnr.createReadStream('test_path', {coerce:true, filter:['a', 'c']});
    s.on('error', function(err){ throw err; });

    var counter = 0;
    s.on('data', function(obj){
      for(var key in obj){
        assert.strictEqual(obj[key], expected[counter][key]);          
      }
      counter++;
    });

    s.on('end', done);       

  });


  it('should stream an SDF resource as line delimited json (as Buffer)', function(done){

    var expected = [
      {date: '2012-08-02', a: '6.2',    b: '10',   c: '9',    d: '5'},
      {date: '2012-08-16', a: 'null',   b: 'null', c: 'null', d: 'null'},
      {date: '2012-09-20', a: '884.4',  b: '1025', c: '2355', d: '111'},
      {date: '2012-10-04', a: '3076.5', b: 'null', c: '4783', d: '148'}
    ].map(function(x){return new Buffer(JSON.stringify(x) + '\n');});;

    var s = ctnr.createReadStream('test_path', {ldjsonify:true});
    s.on('error', function(err){ throw err; });
    var counter = 0;
    s.on('data', function(data){ 
      assert.deepEqual(data, expected[counter++]); 
    });

    s.on('end', done);
  });

});
