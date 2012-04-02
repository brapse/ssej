/*
 * TODO
 * ----
 * Default parser, 
 * - get each line, Json.parse
 * - General command line option parser
 * - instruction parsing
 * - Option loading from dotfiles
 *     - Modules with support functions
 *
 * */


var program = require('commander'),
    LineStream = require('./lib/line_stream').LineStream,
    AggregateFiles = require('./lib/aggregate_files').AggregateFiles;

sum = function (group) {
    return group.reduce(function (accu, e) { return  accu + e }, 0);
}

// From a predicate statement, return a filter function
var generateFunc = function (predicate) {
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

// return a hash function
var extractAggregate = function (aggText) {
    if (!/\[.*\]/.test(aggText)) {
        aggText = '[' + aggText + ']';
    }

    var aggText = 'return ' + aggText + '.join("-")';

    return new(Function)('line', aggText);
};

program
  .version('0.0.1')
  .usage('[options] <map statement> <file ...>')
  .option('-f, --filter [statement]', 'filter statement', generateFunc, function () { return true})
  .option('-m, --map [statement]', 'map statement', generateFunc, function (f) { return f})
  .option('-c, --count [statement]', 'count statement', extractAggregate)
  .option('-c, --group [statement]', 'group statement', extractAggregate)
  .option('-c, --reduce [statement]', 'group statement', extractReduce)
  .option('-p, --parser [parseFunc]', 'a parser function, like JSON.parse maybe', generateParseFunc, function (f) { return f})
  .parse(process.argv);

// ssej --filter='something' myFile
// cat some_dir_*|ssej 'line.length'

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
        return process.stdin;
    }
})();


var lineStream = new LineStream;
inputStream.pipe(lineStream);

var accumulate = {};
var groups = {};

if (program.count) {
    lineStream.on('end', function () {
        console.log(accumulate);
    });
};

if (program.group) {
    lineStream.on('end', function () {
        var result = {};
        Object.keys(groups).forEach(function (key) {
            result[key] = program.reduce(groups[key]);
        });
        console.log(result);
    });
};

lineStream.on('data', function (line) {
    var parsed = program.parser(line);
    if (program.filter(parsed)) {
        if (program.count) {
            var key = program.count(parsed);
            if (accumulate[key]) {
                accumulate[key] += 1;
            } else {
                accumulate[key] = 1;
            }
        } else if(program.group) {
            var key = program.group(parsed);
            if (!groups[key]) {
                groups[key] = []
            }

            groups[key].push(parsed);
        } else {
            console.log(program.map(parsed));
        }
    }
});
