#!/bin/bash

while read SYMBOL; do
  ./get-recent-for-symbol.sh $SYMBOL historical
  sleep 1
done < working-symbols.txt
