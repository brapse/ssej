var AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var streams = require('./lib/streams');
var LineStream = streams.LineStream,
    Map = streams.Map,
    Filter = streams.Filter,
    Group = streams.Group,
    Reduce = streams.Reduce,
    ProxyStream = streams.ProxyStream;

var program = require('./lib/configuration').program;

var options = {
    '-m': Map,
    '--mapper': Map,
    '-f': Filter,
    '--filter': Filter,
    '-g': Group,
    '--group': Group,
    '-s': ProxyStream,
    '--stream': ProxyStream,
    '-r': Reduce,
    '--reduce': Reduce
};

var operator = function (flag) {
    return Object.keys(options).reduce(function (_, k) {
        return k === flag ? options[k] : _;
    }, false);
}

var pipeline = new LineStream;
var inputFiles = [];

var args = process.argv.slice(2); // get the arguments portion

for (var i=0; i < args.length; i++) {
    var op = operator(args[i]);
    if (op) {
        pipeline.tail.pipe(new(op)(args[++i]));
    } else {
        inputFiles.push(args[i]);
    }
}

var inputStream = (function () {
    if (inputFiles.length > 0) {
        return new(AggregateFiles)(inputFiles);
    } else {
        process.stdin.resume();
        process.stdin.setEncoding('utf8');
        return process.stdin;
    }
})();

inputStream.pipe(pipeline.head);

// Connect to output

var util = require('util')
console.warn('DEBUG', pipeline.debug());

// output
pipeline.tail.on('data', function (d) {
  if (typeof d === 'object') {
    console.log(JSON.stringify(d));
  }else {
    console.log(d);
  }
});

