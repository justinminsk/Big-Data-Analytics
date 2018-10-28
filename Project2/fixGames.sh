#!/bin/bash

line=$(cat game_info)
echo $line | sed 's/\(id,[[:alpha:]]\{3\}[[:digit:]]\{9\}\)/\n\1/g' > temp
sed '/^$/d' < temp > temp2
sed 's/\r /*EOL*/g' < temp2 > temp3
sed 's/\*EOL\*^$//' < temp3 > games