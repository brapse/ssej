SSEJ
====

ssej is a toy stream processesing tool you can use from the command line to work with text.

Usage
-----

# Maps
$ cat /usr/share/dict/words|ssej -m 'line.length' 

# Filters: list only short words
$ cat /usr/share/dict/words|ssej -f 'line.length < 10'

# Group: split the dictionary into short and long words
$ cat /usr/share/dict/words|ssej -g 'line.length < 10'

# Reduce: Group words by first letter, select longest word of group
$ cat /usr/share/dict/words|ssej -g "line[0]" -r "(p||'').length > c.length ? p : c"

Notes
-----

Pronouced "essay"

License
-------

Copyright (c) 2012 Sean Braithwaite, See LICENSE.txt for further details.
