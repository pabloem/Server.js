#!/bin/bash
if [ "$#" -eq 4 ]; then
    cp -r $1 $1_1
    cp -r $2 $2_1
fi
cd ../hdt-iris/ 
./bin/consolidate.sh $*
if [ "$#" -eq 4 ]; then
    rm -r $1_1
    rm -r $2_1
fi
