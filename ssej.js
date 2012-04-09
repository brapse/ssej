var LineStream = require('./lib/line_stream').LineStream,
    AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var program = require('./lib/configuration').program;

var inputStream = (function () {
    if (program.inputFiles.length > 0) {
        return new(AggregateFiles)(program.inputFiles)
    } else {
        process.stdin.resume();
        process.stdin.setEncoding('utf8');
        return process.stdin;
    }
})();

var lineStream = new LineStream;
inputStream.pipe(lineStream);

var groups = {};

if (program.count) {
    program.group = program.count;
    program.store = function (e) { return 1 };
    program.reduce = function (group) {
        return group.reduce(function(sum, i) { return sum + i }, 0);
    }
};

if (program.group) {
    lineStream.on('end', function () {
        var result = {};
        Object.keys(groups).forEach(function (key) {
            console.log(JSON.stringify({group: key, value: program.reduce(groups[key])}));
        });
    });
};

lineStream.on('data', function (line) {
    var parsed = program.parser(line);
    if (program.filter(parsed)) {
        if (program.group) {
            var key = program.group(parsed);
            if (!groups[key]) {
                groups[key] = []
            }

            groups[key].push(program.store(parsed));
        } else {
            console.log(program.map(parsed));
        }
    }
});
