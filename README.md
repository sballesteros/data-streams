data-streams
============

Transforms the resources of a datapackage into streams

[![NPM](https://nodei.co/npm/data-streams.png)](https://nodei.co/npm/data-streams/)


Usage
=====

    var Dpkg = require('data-streams');
    var myDataPackage = require('package.json');

    var dpkg = new Dpkg(myDataPackage, '.');
    var stream = dpkg.createReadStream(name, options);


```name``` is the name of the resource.

For the resources in ```csv``` and ```ldjson``` format, an
```options``` object with the following properties can be specified:

- objectMode: (true/false) return a stream in objectMode where every chunk will be a row (as a JS object).
- coerce: (true/false) coerce the values according to the types specified in schema (implies objectMode)
- ldjsonify: (true/false) JSON.stringify + '\n' (implies objectMode)
- filter: an array of column to be kept (all the other will be filtered out)

Tests
=====

    npm test


Licence
=======

MIT
