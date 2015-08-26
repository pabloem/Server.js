#!/bin/bash
# This script is in charge of calling the executable that consolidates removed, 
# added and original HDT triples into the new HDT file.

# First of all, we copy the auxiliary databases into new directories
if [ "$#" -eq 4 ]; then
    cp -r $1 $1_1
    cp -r $2 $2_1
fi
# Then we go into the hdt-iris directory.
# NOTE - Change the following line to point to the HDT IRIS directory in your machine
cd ../hdt-iris/ 

# Then calls the consolidator script in the HDT IRIS directory
./bin/consolidate.sh $*

# Finally, once the new HDT file has been created, we delete the copies
# of the auxiliary databases.
if [ "$#" -eq 4 ]; then
    rm -r $1_1
    rm -r $2_1
fi
