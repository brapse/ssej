var AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var helpers = require('./lib/helpers');

var streams = require('./lib/streams'),
    p = require('./lib/pipeline');

var ConstructFlow= p.ConstructFlow,
    ConstructPipeline = p.ConstructPipeline,
    IPCChannel = streams.IPCChannel,
    Tap = streams.Tap,
    Sink = streams.Sink;

var args = process.argv.slice(2); // get the arguments portion

var files = [],
    definition = [];

// Parse args
for (var i=1; i < args.length; i++) {
    var flag = args[i-1];
    if (flag && flag[0] !== '-') {
        files.push(args[i]);
    } else {
        definition.push([flag, args[i]]);
    }
}

// global for now
isChild = typeof(process.env['NODE_CHANNEL_FD']) === 'string';

// Prepare pipe
var pipeline = (function () {
    if (isChild) {
        return ConstructPipeline(definition);
    } else {
        return ConstructFlow(definition);
    }
})();

// Prepare input
var ls = new streams.LineStream;
var input = (function  () {
    if (files.length > 0) {
        var aggFiles = new(AggregateFiles)(files);
        return aggFiles.pipe(ls);
    } else if (isChild) {
        return new IPCChannel;
    } else {
        process.stdin.setEncoding('utf8');
        var stdin = new(Tap)('STDIN', process.stdin);
        stdin.pipe(ls);

        process.stdin.resume();

        return ls;
    }
})();

// Setup output
var outputHandler = (function () {
    if (isChild) {
        return new(Sink)('IPC OUT', process.send);
    } else {
        return new(Sink)('STD OUT', console.log);
    }
})();

var title = isChild ? 'CHILD' : 'PARENT';

input.pipe(pipeline.head);
pipeline.tail.pipe(outputHandler);

console.log(title, ':', pipeline.head.debug());
