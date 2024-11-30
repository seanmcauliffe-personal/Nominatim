#!/bin/bash -xv

# Derived from https://gist.github.com/RhinoDevel/8a35ebd2a08166f328eca01ab005c6de and edited to work with Pyosmium
# Related to https://github.com/osm-search/Nominatim/issues/1683

# Steps being followed:

#     *) Get the diff file from server
#         1) pyosmium-get-changes (with -f sequence.state for getting sequenceNumber)

#     *) Import diff
#         1) utils/update.php --import-diff

#     *) Index for all the countries at the end

# Hint:
#
# Use "bashdb ./update_database.sh" and bashdb's "next" command for step-by-step
# execution.

# ******************************************************************************

# REPLACE WITH LIST OF YOUR "COUNTRIES":
#
COUNTRIES="europe/monaco europe/andorra"

UPDATEBASEURL="https://download.geofabrik.de"
UPDATECOUNTRYPOSTFIX="-updates"

# If you do not use Photon, let Nominatim handle (re-)indexing:
#
FOLLOWUP="nominatim index"
#
# If you use Photon, update Photon and let it handle the index
# (Photon server must be running and must have been started with "-database",
# "-user" and "-password" parameters):
#
#FOLLOWUP="curl http://localhost:2322/nominatim-update"

# ******************************************************************************
UPDATEDIR="update"

for COUNTRY in $COUNTRIES;
do
    echo "===================================================================="
    echo "$COUNTRY"
    echo "===================================================================="
    DIR="$UPDATEDIR/$COUNTRY"
    FILE="$DIR/sequence.state"
    BASEURL="$UPDATEBASEURL/$COUNTRY$UPDATECOUNTRYPOSTFIX"
    FILENAME=${COUNTRY//[\/]/_}

    echo "Attempting to get changes"
    rm -f "${DIR}"/"${FILENAME}".osc.gz
    pyosmium-get-changes -o "${DIR}"/"${FILENAME}".osc.gz -f "${FILE}" --server "$BASEURL" -v

    echo "Attempting to import diffs"
    nominatim add-data --diff "${DIR}"/"${FILENAME}".osc.gz
done

echo "===================================================================="
echo "Reindexing" 
${FOLLOWUP}
echo "===================================================================="
