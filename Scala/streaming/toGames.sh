#!/bin/bash

LINE=$(cat records)
echo $LINE | sed 's/\(id,[[:alpha:]]\{3\}[[:digit:]]\{9\}\)/\n\1/g' > edit1
cat edit1 | sed '/^$/d' > edit2
cat edit2 | sed 's/\r /*EOL*/g' > edit3
grep 'PHI' < edit3 > philGames

rm edit3
rm edit2
rm edit1

gsutil -m cp philGames gs://justinminsk_bucket/streaming/philGames

rm philGames