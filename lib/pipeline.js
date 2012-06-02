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
    BaseStream.apply(this);

    this.name = 'CHANNEL';
    this._open = false;
    this.definition = [def];
    this.naitive = !(parallel.indexOf(def[0]) !== -1);
};

util.inherits(Channel, BaseStream);

Channel.prototype.connect = function (sectinDef) {
    var naitive = parallel.indexOf(sectionDef[0]) !== -1;

    if (this.naitive == naitive) {
        this.definition.push(sectionDef);
        return true;
    } else {
        this.open();
        return false;
    }

}

var cmdMap = {'-f': 'FILTER',
              '-m': 'MAP' }

Channel.prototype.open = function (input) {
    // Initialize the underlying

    var that = this;
    this.under = (function () {
        if (that.naitive) {
            var p  = ConstructPipeline(that.definition);
            return p
        } else {
            var w = new(Worker)(that.definition);
            return  w;
        }
    })();
    this.name = this.definition.reduce(function (sum, current) {
        return sum + '=>' + cmdMap[current[0]] + '[' + current[1] + ']';
    },'CHANNEL ');

    this._open = true;
}

Channel.prototype.write = function (data) {
    if (!this._open) this.open();
    return this.under.write(data);
}

Channel.prototype.pipe = function (destination) {
    if (!this._open) this.open();
    return this.under.pipe(destination);
}

Channel.prototype.end = function () {
    this.under.end();
}


/* Flow: High level abstraction, contains naitive and foreign section
 */
var ConstructFlow = this.ConstructFlow = function (definition) {
    if (!definition || !Array.isArray(definition)) {
        throw new(Error)('Required a definition!', definition)
    }
    var pipe = new(Channel)(definition.shift());

    definition.forEach(function (block) {
        if (!pipe.connect(block)) {
            var n = new(channel)(block);
            that.pipe = pipe.tail.pipe(n);
        }
    });

    return pipe.head;
};

var operator = function (definition) {
    var op = options[definition[0]];
    if (!op) throw new(Error)('No option operator for: ' + definition[0]);

    return new(op)(definition[1]);
}

/* Pipeline: Low level group of operators
 */
var ConstructPipeline = this.ConstructPipeline= function (definition) {
    var head = operator(definition.shift(0));
    return definition.reduce(function (pipeline, current) {
        var op = operator(current);
        return current.tail.pipe(op);
    }, head);
};

var cp = require('child_process');

// Worker should be renamed
var Worker = this.Worker = function (definition) {
    BaseStream.apply(this);
    // Assume this is running in another process
    
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
    })();

    var defs = this.definition.reduce(function (sum, c) {
        return sum.concat(c);
    }, []);

    var that = this;
    for (var i=0; i < numCPUs; i++) {
        var p = partitions[i] || [];
        var n = cp.fork(__dirname + '/../cli.js', defs.concat(p));

        n.on('exit', function () {
            // Might need to check for invalid worker death
            console.log('WORKER EXIT');
        });

        n.on('message', function (data) {
            that.emit('data', data);
        });

        this.children.push(n);
    }

    this._open = true;
}

Worker.prototype.end = function () {
    console.log('WORKER END');
    // try this first
    this.children.forEach(function (child) {
        console.log('KILLING CHILD');
        child.kill();
    });
}

var c=0;
Worker.prototype.write = function (data) {
    if (!this._open) this.open();
    // Round robin
    var current = c++ % this.children.length;
    this.children[current].send(data);
};
