#!/bin/bash

cat row_sql.txt | sed 's/|//g'  > temp.txt
cat temp.txt 	| sed 's/\([[:alpha:]]*\)  varchar(255)  YES        NULL/\1,/g' > temp2.txt
cat temp2.txt | sed 's/\([[:alpha:]]*\)   varchar(255)  YES        NULL/\1,/g' > temp3.txt
cat temp3.txt | sed 's/\([[:alpha:]]*\)          int(11)       YES        NULL/\1,/g' > temp4.txt
cat temp4.txt | sed 's/\([[:alpha:]]*\)     int(11)       YES        NULL/\1,/g' > temp5.txt
cat temp5.txt | sed 's/\(GIDP\)		 varchar(255)  YES        NULL/\1/g' | sed 's/ //g' |tr -d "\n"  > header.txt

rm temp.txt
rm temp2.txt
rm temp3.txt
rm temp4.txt
rm temp5.txt