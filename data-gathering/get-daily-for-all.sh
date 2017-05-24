#!/bin/bash

mkdir daily-historical

while read SYMBOL; do
  ./get-daily-for-symbol.sh $SYMBOL daily-historical
  sleep 1
done < working-symbols.txt
