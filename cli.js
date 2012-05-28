var AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var helpers = require('./lib/helpers');

var streams = require('./lib/streams');
var LineStream = streams.LineStream,
    Map = streams.Map,
    Filter = streams.Filter,
    Group = streams.Group,
    Reduce = streams.Reduce,
    ProxyStream = streams.ProxyStream;

// Seperat the input files from the test
// ssej -m line.length file_1 file_2 file_3

var args = process.argv.slice(2); // get the arguments portion

var files = [],
    definition = [];

for (var i=1; i < args.length; i++) {
    var flag = args[i-1];
    if (flag && flag[0] !== '-') {
        files.push(args[i]);
    } else {
        definition.push([flag, args[i]]);
    }
}


// XXX: extract pipeline options

// If master
// => Init entire flow
// if Worker
// => Init a raw pipeline
var cluster = require('cluster');

var handler = cluster.isMaster ? Flow : Pipeline;
// Raw pipelines can either handle stdin or file reads

if (files.length > 0) {
    handler.run(files);
} else {
    handler.run(process.stdin);
    process.stdin.resume();
}


handler.open(input);


/* Handling files vs handling stdin
 * The flow will get all the files and schedule */
 
/* Current Design
 * clj executes master process with definition and input
 * definition: [command, spec]
 * input either a list of files or nothing (stdin)
 *
 * Master process initializes a flow
 * Flow creates channels that can either run in parallel or serialized on master
 * channels are either: Pipelines
 *                      Workers
 *
 * Piplines are a bunch of Streams joined together, (Filter, Map, etc)
 * Workers lunch child processes which run Piplines
 * Workers round robin writes over child processes
 *
 * 
