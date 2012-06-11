var fs = require('fs');

/*  Split a file into parallizable chunks
 *  Given a file, and a number n, provide n ranges of bytes
 *  to read the file in parallel */
var splitFile = this.splitFile = function (filename, splits) {
    var size = fs.statSync(filename).size;
    var fd = fs.openSync(filename, 'r');

    var readAhead = 8; // 8K
    var buf = new(Buffer)(readAhead);
    var partSize = Math.floor(size/splits); 
    var partitions = [];

    // search through all splits, except the last one
    // which is assumed to be terminated by EOF
    
    var offset = 0;
    for (var i =0; i < splits -1; i++) {
        // it should NOT search towards the end of the file
        // |  search | search | skip |
        var search_start = offset;
        offset += partSize;
        var search_end   = Math.min(offset + partSize, size);

        offset = (function () {
            //console.log('start: ', search_start, 'end: ', search_end, ' offset: ', offset);
            // Linear probing
            
           var readSize = readAhead;
           while (offset < search_end) {
               //read
               var data = fs.readSync(fd, readSize, offset, 'utf8');

               var loc = data[0].indexOf('\n');
               // search
               if (loc > -1) {
                   return offset +loc;
               }

               // probe forward
               offset += readSize;
               if ((search_end-offset) < readSize) {
                   readSize = offset - search_end; // ensure no over read
               }
           }
        })();

        if (!offset) throw new(Error)('Linear probing failed, possible file format error');

        partitions.push([search_start, offset]);
        offset++; // skip  over newline
    }

    // add last
    partitions.push([offset,size]);

    return partitions;
};
