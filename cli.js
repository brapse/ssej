var AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var helpers = require('./lib/helpers');

var streams = require('./lib/streams'),
    p = require('./lib/pipeline');

var Flow = p.Flow,
    Pipeline = p.Pipeline;


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

isChild = typeof(process.env['NODE_CHANNEL_FD']) === 'string';
if (isChild) {
    //console.log('CHILD');
    console.log(args);
} else {
    console.log('MASTER');
    console.log(args);
}

var handler = new(isChild ? Pipeline : Flow)(definition);
// Raw pipelines can either handle stdin or file reads
//

if (files.length > 0) {
    handler.open(files);
} else {
    handler.open(process.stdin);
    process.stdin.resume();
}

// XXX Child processes arn't getting stdin

// Now we just need to direct the output to the right place
