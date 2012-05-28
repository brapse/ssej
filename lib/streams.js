var stream = require('stream'),
    fs = require('fs');

var Stream = require('stream').Stream,
        util = require('util');

var extractFunc = function (predicate) {
    var funcText = 'return ' +  predicate + ';'

    return new(Function)('line', funcText);
};

var BaseStream = function () {
    Stream.apply(this, arguments);

    this.readable = true;
    this.writable = true;
};

util.inherits(BaseStream, Stream);

BaseStream.prototype.write = function (data) {
    if (this.proxy) {
      this.proxy.write(data);
    } else {
      this.emit('data', data);
    }
};

BaseStream.prototype.pipe = function (into) {
    if (!into) {
      throw new(Error)('trying to pipe into nothing:', this.name);
    }
    Stream.prototype.pipe.apply(this, arguments);

    into.prev = this;
    this.next = into;

    return into;
};

BaseStream.prototype.__defineGetter__('head', function () {
    return this.prev ? this.prev.head : this;
});

BaseStream.prototype.__defineGetter__('tail', function () {
    return this.next ? this.next.tail: this;
});

BaseStream.prototype.debug = function () {
    return this.name + (this.next ? ' => ' + this.next.debug() : ' => OUTPUT');
}

BaseStream.prototype.end = function () {
      this.emit('end');
};

var proxy = function (event, from, to) {
  from.on(event, function (arg) {
    to.emit(arg);
  });
}

var ProxyStream = this.ProxyStream = function (text) {
    BaseStream.apply(this);

    var target = eval(text);
    this.name = 'PROXY(' + text + ')';

    var that = this;
    console.log(target);
    that.proxy = target;
    target.on('data', console.log);
    //proxy('data', target, that);
    //proxy('end', target, that);
    //proxy('close', target, that);
}

util.inherits(ProxyStream, BaseStream);

ProxyStream.prototype.write = function (data) {
    this.proxy.write(data);
};

var LineStream = this.LineStream = function (input) {
    BaseStream.apply(this);

    this.name = 'LineStream';
    this.buffer = '';

    if (input) {
        if (typeof(input) === 'string') {
            // assume file type
            var source = fs.createReadStream(inputFile);
            source.pipe(this);
        } else {
            // assume stdint
            source.pipe(this)
        }
    }
}

util.inherits(LineStream, BaseStream);

LineStream.prototype.write = function (data) {
    var that = this;
    data = data.toString('utf8');
    var index = data.indexOf('\n');

    if (index > 0) {
        data = this.buffer + data;
        var lines = data.split('\n');
        this.buffer = lines.pop();

        lines.forEach(function (line) {
            that.emit('data', line);
        });
    } else {
        this.buffer = this.buffer + data;
    }
}

var Filter = this.Filter = function (funcText) {
    BaseStream.apply(this);

    this.name = 'FILTER(' + funcText + ')';
    this.func = extractFunc(funcText);
};

util.inherits(Filter, BaseStream);

Filter.prototype.write = function (data) {
    if (this.func(data)) {
        this.emit('data', data);
    }
}

var Map = this.Map = function (funcText) {
    BaseStream.apply(this);

    this.name = 'MAP (' + funcText + ')';
    this.func = extractFunc(funcText);
};

util.inherits(Map, BaseStream);

Map.prototype.write = function (data) {
    this.emit('data', this.func(data));
};

var identity = function (e) { return e };

var Group = this.Group = function (funcText) {
    BaseStream.apply(this);

    this.hash = extractFunc(funcText);

    this.name = 'GROUP (' + funcText + ')';
    this.groups = {};
    this.store = identity;
};

util.inherits(Group, BaseStream);

Group.prototype.__defineSetter__('next', function (next) {
  if (next.constructor === Reduce) {
    this.reduce = next.reduce;
  }
  this._next = next;
});

Group.prototype.__defineGetter__('next', function (next) {
  return this._next;
});


Group.prototype.write = function (data) {
    // add a reduce here
    var key = this.hash(data);
    if (this.reduce) {
      this.groups[key] = this.reduce(this.groups[key], data);
    } else {
      if (!this.groups[key]) {
          this.groups[key] = [this.store(data)];
      } else {
          this.groups[key].push(this.store(data));
      }
    }
};

Group.prototype.end = function () {
    var that = this;
    Object.keys(this.groups).forEach(function (key) {
        that.emit('data', {group: key, value: that.groups[key]});
    });
};

var Reduce = this.Reduce = function (funcText) {
  BaseStream.apply(this, arguments);

  this.name = 'REDUCE (' + funcText + ')';
  funcText = 'return ' + funcText;
  this.reduce = new(Function)('p', 'c', 'i', 'list', funcText);
};

util.inherits(Reduce, BaseStream);

var EntryPoint = this.EntryPoint = function (input) {
    BaseStream.apply(this);

    this.writable = true;
    this.readable = true;

    this.name = 'EntryPoint';
}

util.inherits(EntryPoint, BaseStream);

EntryPoint.prototype.write = function (inputFile) {
    var sections = this.pipeline.sections();
    var head;
    sections.forEach(function (section) {
        if (section.parallel) {
            // XXX: This should be sensitive to worker pool, we don't want to be spawning
            // A bunch of workers just because we have lots to do
            // Spawn this part of the pipeline in a seperate thread
            var sec = new(Worker)(section.definition);

            // Connect the pipes the do IPC
            head = head.pipe(sec);
        } else {
            // connection them regularly
            head = head.pipe(section);
        }
    });

    // start a new child process child process for the parallizable parts
    // Aggregate the result in some combiner
    //
    // Imagine the following
    // Parallel => output
    // Parallel => aggregate => output
    // parallel, aggregate, parallel => output
    //
    // SOLUTION
    // Only Spawn new processes for the parallel parts, use this process for the 
    // aggregate parts. Modify the streams so they can read and write from IPC
}

var IPCPipe = this.IPCPipe = function (parent) {
    BaseStream.apply(this);

    this.parent = parent;
}

util.inherits(IPCPipe, BaseStream);

IPCPipe.prototype.write = function (data) {
    // XXX figure ut serialization
    this.parent.send(data);
};


// Muxer: many to one
// Child processes will all be writting to this stream
// many to one

// May require some kind of stream id
var Muxer = this.Muxer = function (from, to) {
    BaseStream.prototype.apply(this);
    var that = this;

    // from is the worker representation
    // to  the naitive next step
}

util.inherits(Muxer, BaseStream);


// Demuxer
// one to many
var Demuxer = this.Demuxer = function () {
}

Demuxer.prototype.subscribe(stream) {
    this.scubscriptions.push(stream);
}

//
