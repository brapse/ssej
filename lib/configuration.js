
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

var parameters = ['-f', '-m', '-r', '-g'],
    flags      = ['-d'];

var parseArgs = function (args) {
    var program = {};
    program.files = [];
    program.parameters = [];
    program.flags = [];

    for (var i = 0; i < args.length; i++) {
        var current = args[i];
        if (flags.indexOf(current) > -1) {
            program.flags.push(current);
        } else if (parameters.indexOf(current) > -1) {
            program.parameters.push([args[i], args[++i]]);
        } else {
            program.files.push(current);
        }
    }

    // for now;
    program.definition = program.parameters;

    return program;
};

var program = parseArgs(process.argv.slice(2));

var userConfig = (function () {
    try {
        return require(process.env.HOME + '/.ssejrc.js');
    } catch (e) {
        return {};
    }
}());

/*
 * */
var configs = Object.keys(userConfig).filter(function (key) {
    var matchFiles = (function () {
        if (!userConfig[key].match) { return false; }

        return program.files.filter(function (file) { return userConfig[key].match.test(file); });
    }());

    return key == program.config_preset || matchFiles.length == program.files.length;
}).map(function (k) {
    return userConfig[k];
});

configs = merge.apply(null, configs);

this.program = merge(userConfig.default || {}, configs, program);
