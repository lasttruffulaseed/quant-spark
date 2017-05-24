#!/bin/bash
dt=`date "+%d-%m-%Y"`
curl -o $2/$1-$dt.txt --retry 2 -w "%{filename_effective}\n" "http://www.google.com/finance/getprices?i=60&p=15d&f=d,o,h,l,c,v&df=cpct&q=$1" 
