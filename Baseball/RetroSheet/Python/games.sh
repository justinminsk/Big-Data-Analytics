#!/bin/bash

for i in {1925..2017}
	do
		curl http://www.retrosheet.org/events/${i}eve.zip > data.zip
		unzip data.zip
		rm *.ROS
		rm TEAM*
		rm data.zip
		cat *.E* >> game_info
		rm *.E*
		line=$(cat game_info)
		echo $line | sed 's/\(id,[[:alpha:]]\{3\}[[:digit:]]\{9\}\)/\n\1/g' > temp
		sed '/^$/d' < temp > temp2
		sed 's/\r /*EOL*/g' < temp2 > temp3
		sed 's/\*EOL\*^$//' < temp3 > ${i}games
		gsutil cp ${i}games gs://justinminsk_bucket/classwork/retroGames/
		rm ${i}games
		rm temp
		rm temp2
		rm temp3
		rm game_info
	done