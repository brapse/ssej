var AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var helpers = require('./lib/helpers');

var streams = require('./lib/streams'),
    p = require('./lib/pipeline');

var ConstructFlow= p.ConstructFlow,
    ConstructPipeline = p.ConstructPipeline,
    IPCChannel = streams.IPCChannel;

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
        console.log('Constructing pipe');
        return ConstructPipeline(definition);
    } else {
        console.log('Constructing Flow');
        return ConstructFlow(definition);
    }
})();

// Prepare input
var ls = new streams.LineStream;
var input = (function  () {
    if (files.length > 0) {
        console.log('INPUT FILES', process.pid);
        var aggFiles = new(AggregateFiles)(files);
        return aggFiles.pipe(ls);
    } else if (isChild) {
        console.log('INPUT IPC', process.pid);
        return new IPCChannel;
    } else {
        console.log('INPUT STDIN', process.pid);
        process.stdin.setEncoding('utf8');
        process.stdin.resume();
        process.stdin.pipe(ls);

        return ls;
    }
})();

// Setup output
var outputHandler = (function () {
    if (isChild) {
        console.log('OUTPUT IPC:', process.pid);
        return process.send;
    } else {
        console.log('OUTPUT stdout: ', process.pid);
        return console.log;
    }
})();

input.on('end', function () {
    console.log('END OF INPUT');
    pipeline.close();
});
//console.log(pipeline);
pipeline.on('data',outputHandler);
input.pipe(pipeline);
