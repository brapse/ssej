
var stream = require('stream'),
    fs = require('fs');

var Stream = require('stream').Stream,
        util = require('util');

var extractFunc = function (predicate) {
    var funcText = 'return ' +  predicate + ';';

    return new(Function)('line', funcText);
};

var BaseStream = this.BaseStream = function () {
    Stream.apply(this, arguments);

    this.readable = true;
    this.writable = true;

    this._open = false;
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

BaseStream.prototype.open = function () {
    this.emit('open');
};

BaseStream.prototype.close = function () {
    this.emit('close');
};

BaseStream.prototype.__defineGetter__('head', function () {
    return this.prev ? this.prev.head : this;
});

BaseStream.prototype.__defineGetter__('tail', function () {
    return this.next ? this.next.tail: this;
});

BaseStream.prototype.debug = function () {
    return this.name + (this.next ? ' => ' + this.next.debug() : '');
};

BaseStream.prototype.end = function () {
    this.emit('end');
};

var Tap = this.Tap = function (name, input) {
    BaseStream.apply(this);

    this.name = name;
    var that = this;
    input.on('data', function (data) {
        that.emit('data', data);
    });

    that = this;
    input.on('end', function () {
            //console.log('ENDING TAP BECAUSE OF PARENT END');
        that.end();
    });
};

util.inherits(Tap, BaseStream);

var Sink = this.Sink = function (name, output) {
    BaseStream.apply(this);

    this.name = name;
    this.on('data', function (data) {
        if (process.env['SSEJ_CHILD']) {
            //console.log(output.toString());
        }
        output(data);
    });
};

util.inherits(Sink, BaseStream);

var proxy = function (event, from, to) {
    from.on(event, function (arg) {
        to.emit(arg);
    });
};

var ProxyStream = this.ProxyStream = function (text) {
    BaseStream.apply(this);

    var target = eval(text);
    this.name = 'PROXY(' + text + ')';

    var that = this;
    that.proxy = target;
    //proxy('data', target, that);
    //proxy('end', target, that);
    //proxy('close', target, that);
};

util.inherits(ProxyStream, BaseStream);

ProxyStream.prototype.write = function (data) {
    this.proxy.write(data);
};

var LineStream = this.LineStream = function () {
    BaseStream.apply(this);

    this.name = 'LineStream';
    this.buffer = '';
};

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
};

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
};

var Map = this.Map = function (funcText) {
    BaseStream.apply(this);

    this.name = 'MAP (' + funcText + ')';
    this.func = extractFunc(funcText);
};

util.inherits(Map, BaseStream);

Map.prototype.write = function (data) {
    this.emit('data', this.func(data));
};

var identity = function (e) { return e; };

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
    this.emit('end');
};

var Reduce = this.Reduce = function (funcText) {
    BaseStream.apply(this, arguments);

    this.name = 'REDUCE (' + funcText + ')';
    funcText = 'return ' + funcText;
    this.reduce = new(Function)('p', 'c', 'i', 'list', funcText);
};

util.inherits(Reduce, BaseStream);

var IPCChannel = this.IPCChannel = function () {
    BaseStream.apply(this, arguments);

    this.readable = true;
    this.writable = false;

    this.name = 'IPCChannel';

    var that = this;
    process.on('message', function (data) {
        that.emit('data', data);
    });
};

util.inherits(IPCChannel, BaseStream);
