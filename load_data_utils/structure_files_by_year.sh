#!/bin/bash

SRC_DIR="./raw_data/raw"

DEST_DIR="./raw_data/strctured_data"

for file in "$SRC_DIR"/*; do

    if [[ $(basename "$file") =~ _([0-9]{4})_ ]]; then
        YEAR=${BASH_REMATCH[1]}

        mkdir -p "$DEST_DIR/$YEAR"

        cp "$file" "$DEST_DIR/$YEAR/"

        echo "Copied file $(basename "$file") to $DEST_DIR/$YEAR/"
    else
        echo "No year found in file name $(basename "$file"). Skipping..."
    fi
done