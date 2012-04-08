var program = require('commander'),
    LineStream = require('./lib/line_stream').LineStream,
    AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

var extractFunc = function (predicate) {
    var funcText = 'return ' +  predicate + ';'

    return new(Function)('line', 'group', funcText);
}

var generateParseFunc = function (parseFunc) {
    var funcText = 'return ' + parseFunc+ '(line);'

    return new(Function)('line', funcText);
}

var extractReduce = function (parseFunc) {
    var funcText = 'return ' + parseFunc+ ';';

    return new(Function)('group', funcText);
}


var identity = function (f) { return f };

program
  .version('0.0.1')
  .usage('[options] <map statement> <file ...>')
  .option('-f, --filter [statement]', 'filter statement', extractFunc, function () { return true})
  .option('-m, --map [statement]', 'map statement', extractFunc, identity)
  .option('-c, --count [statement]', 'count [statement]', extractFunc, extractFunc('line'))
  .option('-g, --group [statement]', 'group by [statement]', extractFunc)
  .option('-s, --store [statement]', 'specify what to store in groups', extractFunc, identity)
  .option('-r, --reduce [statement]', 'reduce each group by [statement]', extractReduce, identity)
  .option('-p, --parser [parseFunc]', 'a parser function, like JSON.parse maybe', generateParseFunc, function (f) { return f})
  .parse(process.argv);

var determineFiles = function (files) {
    var notFound = files.filter(function (file) {
        try {
            var stat = fs.statSync(file);
            return true;
        } catch (e) {
            return false;
        }
    });

    if (notFound.length > 0) {
        throw new(Error)('Files not found', notFound.join(' '));
    }

    return files;
};

var inputFiles = determineFiles(program.args);

var inputStream = (function () {
    if (inputFiles.length > 0) {
        return new(AggregateFiles)(inputFiles)
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
        if(program.group) {
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
