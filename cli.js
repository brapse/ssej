var AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var streams = require('./lib/streams');
var LineStream = streams.LineStream,
    Map = streams.Map,
    Filter = streams.Filter,
    Group = streams.Group;

var args = process.argv.slice(2); // get the arguments portion

var options = {
    '-m': Map,
    '--mapper': Map,
    '-f': Filter,
    '--filter': Filter,
    '-g': Group,
    '--group': Group
};


Object.prototype.only = function () {
    var n = {};
    var that = this;
    var keys = Array.prototype.slice.apply(arguments);
    keys.forEach(function (k) {
        n[k] = that[k];
    });

    return n;
}

var operator = function (flag) {
    return Object.keys(options).reduce(function (_, k) {
        return k === flag ? options[k] : _;
    }, false);
}

var pipeline = new LineStream;
var inputFiles = [];

for (var i=0; i < args.length; i++) {
    var op = operator(args[i]);
    if (op) {
        pipeline.tail.pipe(new op(args[++i]));
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

console.warn('DEBUG', pipeline.debug());
pipeline.tail.on('data', console.log);
