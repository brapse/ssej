SSEJ
====

ssej is a multicore stream processing tool you can use from the command line to work with text.

Usage
-----

### Maps

```shell
$ cat /usr/share/dict/words|ssej -m 'line.length'
```
### Filters: list only short words

```shell
$ cat /usr/share/dict/words|ssej -f 'line.length < 10'
```

### Group: split the dictionary into short and long words

```shell
$ cat /usr/share/dict/words|ssej -g 'line.length < 10'
```

### Reduce: Group words by first letter, select longest word of group

```shell
$ cat /usr/share/dict/words|ssej -g "line[0]" -r "(p||'').length > c.length ? p : c"
```

Notes
-----

Pronounced "essay"

License
-------

Copyright (c) 2012 Sean Braithwaite, See LICENSE.txt for further details.
