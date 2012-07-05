
var AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var streams = require('./lib/streams'),
    p = require('./lib/pipeline');

var ConstructFlow = p.ConstructFlow,
    ConstructPipeline = p.ConstructPipeline,
    IPCChannel = streams.IPCChannel,
    Tap = streams.Tap,
    Sink = streams.Sink;

// global for now
var isChild = typeof(process.env.NODE_CHANNEL_FD) === 'string';

var program = require('./lib/configuration').program;

// Prepare pipe
var pipeline = (function () {
    if (isChild) {
        return ConstructPipeline(program.definition);
    } else {
        return ConstructFlow(program.definition);
    }
}());

// Prepare input
var ls = new streams.LineStream();
var input = (function  () {
    if (program.files.length > 0) {
        var aggFiles = new(AggregateFiles)(program.files);
        return aggFiles.pipe(ls);
    } else if (isChild) {
        return new IPCChannel();
    } else {
        process.stdin.setEncoding('utf8');
        var stdin = new(Tap)('STDIN', process.stdin);
        stdin.pipe(ls);

        process.stdin.resume();

        return ls;
    }
}());

// Setup output
var outputHandler = (function () {
    if (isChild) {
        return new(Sink)('IPC OUT', process.send);
    } else {
        return new(Sink)('STD OUT', console.log);
    }
}());

var title = isChild ? 'CHILD' : 'PARENT';

input.pipe(pipeline.head);
pipeline.tail.pipe(outputHandler);

if (program.flags.indexOf('-d') > -1) {
    console.log(title, ':', pipeline.head.debug());
}
