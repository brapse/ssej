// helpers
var merge = function () {
    var result = {};
    var args = Array.prototype.slice.apply(arguments);

    args.forEach(function (el) {
        Object.keys(el).forEach(function (key) {
            result[key] = el[key];
        });
    });

    return result;
};

var extractKeyConfig = function (input) {
};

var extractFunc = function (predicate) {
    var funcText = 'return ' +  predicate + ';'

    return new(Function)('line', 'group', funcText);
};

var generateParseFunc = function (parseFunc) {
    var funcText = 'return ' + parseFunc+ '(line);'

    return new(Function)('line', funcText);
};

var extractReduce = function (parseFunc) {
    var funcText = 'return ' + parseFunc+ ';';

    return new(Function)('group', funcText);
};

var identity = function (f) { return f };

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

    return files
};


/* Composable operations
 * ------------
 * it would be nice if you could compose arguments together
 *     $ cat myFile|ssej -m JSON.parse(line).event -g line -f group.length > 2
 * In this case, you can filter on the groups.
 * ALSO the --parser argument goes away since it's just a map
 *
 * */
// Parse command line options
var program = require('commander');

program
  .version('0.0.1')
  .usage('[options] <map statement> <file ...>')
  .option('-f, --filter [statement]', 'filter statement', extractFunc, function () { return true})
  .option('-m, --map [statement]', 'map statement', extractFunc, identity)
  .option('-c, --count [statement]', 'count [statement]', extractFunc, extractFunc('line'))
  .option('-g, --group [statement]', 'group by [statement]', extractFunc)
  .option('-s, --store [statement]', 'specify what to store in groups', extractFunc, identity)
  .option('-r, --reduce [statement]', 'reduce each group by [statement]', extractReduce, identity)
  .option('-p, --parser [parseFunc]', 'a parser function, like JSON.parse maybe', generateParseFunc)
  .option('-cp, --config-preset [config key]', 'a key to include presets from your ~/.ssejrc.js file', extractKeyConfig, {})
  .parse(process.argv);

program.inputFiles = determineFiles(program.args) || 'stdin';

// Config is a set of keys of configs that should apply
//
var userConfig = (function () {
    try {
        return require(process.env['HOME'] + '/.ssejrc.js');
    } catch (e) {
        return {};
    }
})();

var configs = Object.keys(userConfig).filter(function (key) {
    var matchFiles = (function () {
        if (!userConfig[key].match) { return false }

        return program.inputFiles.filter(function (file) { return userConfig[key].match.test(file) });
    })();

    return key == program.config_preset || matchFiles.length == program.inputFiles.length;
}).map(function (k) {
    return userConfig[k];
});

configs = merge.apply(null, configs);

/* Merge Configuration
 *
 * Default should be applied first,
 * then matching
 * then command line arguments
 * */

this.program = merge(userConfig.default || {}, configs, program);
