#!/bin/bash
#note: added the sed to replace dots with dashes for symbol names in yahoo request
day=`date -j +"%d"`
mth=`date -j +"%m"`
yr=`date -j +"%Y"`
sym=`echo $1 | sed -e 's/\./-/g'`
curl -o $2/$1.txt --retry 2 -w "%{filename_effective}\n" "http://real-chart.finance.yahoo.com/table.csv?s=$sym&a=00&b=2&c=2000&d=$mth&e=$day&f=$yr&g=d&ignore=.csv" 
