var Stream = require('stream').Stream,
        util = require('util'),
        fs = require('fs');

var AggregateFiles = this.AggregateFiles = function (files) {
    Stream.apply(this);

    this.readable = true;

    if (!files || files.length === 0) {
        throw new(Error)('Aggreagate files requires files to aggregate');
    }

    var that = this;
    that.files = files;
    var tryCreateStream = function (dest) {
        if (that.files.length > 0) {
            that.active = fs.createReadStream(that.files.pop());
            that.active.on('data', function (data) { that.emit('data', data) });
            that.active.on('error', function (data) { that.emit('error', data) });

            that.active.on('end', tryCreateStream);
        } else {
            that.emit('end');
        }
    }

    tryCreateStream();
}

AggregateFiles.prototype.end = function () {
}

util.inherits(AggregateFiles, Stream);
