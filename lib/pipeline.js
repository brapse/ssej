var helpers = require('./lib/helpers');

var streams = require('./lib/streams');
var LineStream = streams.LineStream,
    Map = streams.Map,
    Filter = streams.Filter,
    Group = streams.Group,
    Reduce = streams.Reduce,
    ProxyStream = streams.ProxyStream;


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
    this.naitive = parallel.indexOf(sectionDef[0]) !== -1;
};

Channel.prototype.connect (sectinDef) {
    var naitive = parallel.indexOf(sectionDef[0]) !== -1;

    // Chain together pipes so long as they are either both naitive, or neither of them
    // are naitive

    if (this.naitive == naitive) {
        this.definition.push(sectionDef);
        return true;
    } else {
        return false;
    }
}

Channel.prototype.open = function (input) {
    // Initialize the underlying
    if (this.naitive) {
        this.pipe = new(Pipeline)(this.definition);
    } else {
        this.pipe = new(Worker)(this.definition);
    }

    this.pipe.open(file);
}


/* Flow: High level strings of channels
 */
var Flow = function (definition) {
    // top level representation
    this.definition = definition;

    // split up the definition into channels
    var channels = [];
    // XXX implement split
    var blocks = split(definition);

    var head = new(Channel)(block.shift());

    blocks.forEach(function (block) {
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


Flow.prototype.open = function (input) {
    this.head.open(input);
};


/* Pipeline: Low level group of operators
 */
var Pipeline = this.Pipeline = function (definition) {
    var pipeline;
    for (var i=0; i < definition.length; i++) {
        var op = options[definition[i]];
        if (!pipeline) {
            pipeline = new(op)(args[++i]);
        } else {
            pipeline.tail.pipe(new(op)(args[++i]));
        }
    }

    this.pipeline = pipeline;
    // TODO proxy methods
};

Pipeline.prototype.open = function (input) {
    var inStream = (function () {
        if (Array.isArray(input)) {
            return new(AggregateFiles)(input);
        } else {
            process.stdin.resume();
            return process.stdin;
        }
    })();

    inStream.pipe(this.head);
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
    if (Array.isArray(input)) {
        var partitions = partitions(input, numCPUs);
    }
   
    for (var i=0; i < numCPUs; i++) {
        var n = cp.fork(__dirname + '/clj.js', this.definition.concat(partitions[i]));
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
