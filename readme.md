[![Build Status](https://travis-ci.com/segmeno/kodo.svg?branch=master)](https://travis-ci.com/segmeno/kodo)

This Project is a lightweight database access layer for quick data inserts, updates, deletes or reads.
Currently supported DBs are MySql.

usage:
let your container object (the table representation) extend from DatabaseEntity and use the annotations from 
com/segmeno/kodo/annotation to further describe your fields.
Instantiate and use the SqlManager to access the DB.