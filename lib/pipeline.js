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
}

var parallel = ['-m', '--map', 
                '-f', '--filter'];

/* Channel: High level collection of naitive and foreign piplines 
 **/
var Channel = function (def) {
    this.definition = [def];
    this.naitive = !(parallel.indexOf(def[0]) !== -1);
};

Channel.prototype.connect = function (sectinDef) {
    var naitive = parallel.indexOf(sectionDef[0]) !== -1;

    if (this.naitive == naitive) {
        this.definition.push(sectionDef);
        return true;
    } else {
        return false;
    }
}

Channel.prototype.open = function (input) {
    console.log('OPEN CHANNEL', this.naitive);
    // Initialize the underlying
    if (this.naitive) {
        this.pipe = new(Pipeline)(this.definition);
    } else {
        this.pipe = new(Worker)(this.definition);
    }

    this.pipe.open(input);
}


/* Flow: High level strings of channels
 */
var Flow = this.Flow = function (definition) {
    BaseStream.apply(this);
    if (!definition || !Array.isArray(definition)) {
        throw new(Error)('Required a definition!', definition)
    }
    // top level representation
    this.definition = Array.prototype.concat.apply(definition);

    // split up the definition into channels
    console.log('DEF:', definition);
    var head = new(Channel)(definition.shift());
    var channels = [head];

    definition.forEach(function (block) {
        if (!head.connect(block)) {
            channels.push(head);
            head = new(channel)(block);
        }
    });

    // connect the channels
    channels.reduce(function (from, to) {
        return from.pipe(to);
    });

    this.head = channels[0];
};


//util.inherits(Flow, BaseStream);

Flow.prototype.open = function (input) {
    console.log('OPEN FLOW');
    this.head.open(input);
};

var operator = function (definition) {
    var op = options[definition[0]];
    if (!op) throw new(Error)('No option operator for: ' + definition[0]);

    return new(op)(definition[1]);
}

/* Pipeline: Low level group of operators
 */
var Pipeline = this.Pipeline = function (definition) {
    BaseStream.apply(this);

    //console.log(definition);
    var pipeline = operator

    var head = operator(definition.shift(0));
    this.pipeline = definition.reduce(function (pipeline, current) {
        var op = operator(current);
        pipeline.tail.pipe(op);
    }, head);
};

util.inherits(Pipeline, BaseStream);

var cluster = require('cluster');

Pipeline.prototype.open = function (input) {
    console.log('PIPELINE OPEN');
    var inStream = (function () {
        if (Array.isArray(input)) {
            return new(AggregateFiles)(input);
        } else {
            console.log('stdin!');
            process.stdin.setEncoding('utf8');
            process.stdin.resume();
            return process.stdin;
        }
    })();

    console.log('CHILD?', isChild);
    if (!isChild) {
        inStream.on('data', console.log);
    }

    // OUTPUT
    this.pipeline.tail.on('data', function (data) {
        if (!isChild) {
            console.log(data);
        } else {
            // send make to master
            process.send(data);
        }
    });

    var ls = new LineStream;
    inStream.pipe(ls).pipe(this.pipeline.head);
};

var cp = require('child_process');

// Worker should be renamed
var Worker = this.Worker = function (definition) {
    // Assume this is running in another process
    
    this.definition = definition;
    this.children = [];
};

var numCPUs = require('os').cpus().length;

var partition = function (arr, num) {
    var res = [];
    for (var i = 0; i < num; i++) {
        res[i] = arr.slice(0, Math.ceil(arr.length / num));
    }

    return arr;
};

Worker.prototype.open = function (input) {
    /* Simplest case: spawn children for each cpu and
     * round robin between them */

    // Input can be files
    //
    var partitions = (function () {
        if (Array.isArray(input)) {
            return partition(input, numCPUs);
        } else {
            return [];
        }
    })();

    var defs = this.definition.reduce(function (sum, c) {
        console.log('D!!!!', c);
        return sum.concat(c);
    }, []);

    for (var i=0; i < numCPUs; i++) {
        var p = partitions[i] || [];
        var n = cp.fork(__dirname + '/../cli.js', defs.concat(p));
        n.on('message', function (data) {
            that.emit('data', data);
        });

        this.children.push(n);
    }
}


var c=0;
Worker.prototype.write = function (data) {
    // Round robin
    var current = c++ % this.children.length;
    this.children[current].send(data);
};
