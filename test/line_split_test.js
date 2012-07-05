var assert = require('assert'),
    fs = require('fs');

var splitFile = require('./../lib/split').splitFile;

var filename = "./test/fixtures/lines.txt";

var splits = splitFile(filename, 2);


var fd = fs.openSync(filename, 'r')
var read = function (start, end) {
    return fs.readSync(fd, end - start, start, 'utf8')[0];
};

var first = read(splits[0][0], splits[0][1]);
assert.equal(first, 'one\ntwo\nthree\nfour\nfive\nsix');

var second= read(splits[1][0], splits[1][1]);
assert.equal(second, 'seven\neight\nnine\nten\n');

console.log('OK!');
