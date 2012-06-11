var Stream = require('stream').Stream,
        util = require('util')

var LineStream = this.LineStream = function () {
    Stream.apply(this);

    this.readable = true;
    this.writable = true;

    this.buffer = '';
}

util.inherits(LineStream, Stream);

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

LineStream.prototype.end = function () {
    this.emit('end');
}
