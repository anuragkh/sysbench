#!/bin/bash

CSVFILE=$1

mongo localhost:27017/bench tools/create_index.js

JSONFILE=${CSVFILE%.*}.json
echo $JSONFILE
python tools/csv2json.py -c $CSVFILE -j $JSONFILE -d '|'
mongoimport --db bench --collection data --file $JSONFILE
rm $JSONFILE
