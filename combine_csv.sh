#!/bin/bash
echo "MessageID,Date,From,To,Body" > /tmp/enronCombined.csv
for i in /tmp/enron12/*.csv ; do
    echo "Processing $i"
    cat $i >> /tmp/enronCombined.csv
    rm $i
done
echo "Done"