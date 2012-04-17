var stream = require('stream');

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
    this.emit('data', data);
};

BaseStream.prototype.pipe = function (into) {
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

var LineStream = this.LineStream = function () {
    BaseStream.apply(this);

    this.name = 'LineStream';

    this.buffer = '';
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

Group.prototype.write = function (data) {
    var key = this.hash(data);
    if (!this.groups[key]) {
        this.groups[key] = [this.store(data)];
    } else {
        this.groups[key].push(this.store(data));
    }
};

Group.prototype.end = function () {
    var that = this;
    console.log('GROUP END', Object.keys(this.groups).length);
    Object.keys(this.groups).forEach(function (key) {
        that.emit('data', {group: key, value: that.groups[key]});
    });
};
