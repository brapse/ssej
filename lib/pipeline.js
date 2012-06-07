
var streams = require('./streams');
var LineStream = streams.LineStream,
    Map = streams.Map,
    Filter = streams.Filter,
    Group = streams.Group,
    Reduce = streams.Reduce,
    ProxyStream = streams.ProxyStream,
    BaseStream = streams.BaseStream;

var util = require('util');

var options = {
    '-m': Map,
    '--mapper': Map,
    '-f': Filter,
    '--filter': Filter,
    '-g': Group,
    '--group': Group,
    '-s': ProxyStream,
    '--stream': ProxyStream,
    '-r': Reduce,
    '--reduce': Reduce
};

var operator = function (flag) {
    return Object.keys(options).reduce(function (_, k) {
        return k === flag ? options[k] : _;
    }, false);
};

var parallel = ['-m', '--map',
                '-f', '--filter'];

var NATIVE  = 'NATIVE',
    FOREIGN = 'FOREIGN';

var affinity = function (flag) {
    return parallel.indexOf(flag) > 0 ? FOREIGN : NATIVE;
};


/* Flow: High level abstraction, contains native and foreign section
 */

var ConstructFlow = this.ConstructFlow = function (definition) {
    if (!definition || !Array.isArray(definition)) {
        throw new(Error)('Required a definition!', definition);
    }

    var start = definition.shift();
    var sections = [[start]];
    var index = 0;
    sections[index].affinity = affinity(start[0]);

    definition.forEach(function (block) {
        var aff = affinity(block[0]);
        if (sections[index].affinity == aff) {
            sections[index].push(block);
        } else {
            sections[++index] = [block];
            sections[index].affinity = aff;
        }
    });

    var materialize = function (sectionDef) {
        if (sectionDef.affinity == NATIVE) {
            return ConstructPipeline(sectionDef);
        } else {
            return new(Worker)(sectionDef);
        }
    };
    var pipe = materialize(sections.shift());
    sections.reduce(function (sum, current) {
        return sum.head.pipe(materialize(current));
    }, pipe);

    return pipe.head;
};

var operator = function (definition) {
    var op = options[definition[0]];
    if (!op) {
        throw new(Error)('No option operator for: ' + definition[0]);
    }

    return new(op)(definition[1]);
};

/* Pipeline: Low level group of operators
 */
var ConstructPipeline = this.ConstructPipeline = function (def) {
    var head = operator(def.shift());
    return def.reduce(function (pipeline, current) {
        var op = operator(current);
        return pipeline.tail.pipe(op);
    }, head).head;
};

var cp = require('child_process');

// Worker should be renamed
var Worker = this.Worker = function (definition) {
    BaseStream.apply(this);

    this.name = 'WORKER DELEGATION';
    this.definition = definition;
    this.children = [];
};

util.inherits(Worker, BaseStream);

var numCPUs = require('os').cpus().length;

var partition = function (arr, num) {
    var res = [];
    for (var i = 0; i < num; i++) {
        res[i] = arr.slice(0, Math.ceil(arr.length / num));
    }

    return arr;
};

Worker.prototype.open = function (input) {
    var partitions = (function () {
        if (Array.isArray(input)) {
            return partition(input, numCPUs);
        } else {
            return [];
        }
    }());

    var defs = this.definition.reduce(function (sum, c) {
        return sum.concat(c);
    }, []);

    var that = this;
    for (var i = 0; i < numCPUs; i++) {
        var p = partitions[i] || [];
        var n = cp.fork(__dirname + '/../cli.js', defs.concat(p));

        n.on('message', function (data) {
            that.emit('data', data);
        });

        this.children.push(n);
    }

    this._open = true;
};

Worker.prototype.end = function () {
    // XXX: might make sense to issue an end instead of HARD KILL
    this.children.forEach(function (child) {
        child.kill();
    });
};

var c = 0;
Worker.prototype.write = function (data) {
    if (!this._open) { this.open(); }
    // Round robin
    var current = c++ % this.children.length;
    this.children[current].send(data);
};
