#!/bin/bash

while read SYMBOL; do
  ls daily-historical/$SYMBOL.txt >nul 2>> rpt.err
done < working-symbols.txt
