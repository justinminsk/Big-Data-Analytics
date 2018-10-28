#!/bin/bash

gsutil cp -r gs://justinminsk_bucket/midterm/betterMidA.csv .

echo label,sent,received,count > betterMid.csv
cat betterMidA.csv/part* >> betterMid.csv

rm -r betterMidA.csv
gsutil rm -r gs://justinminsk_bucket/midterm/betterMidA.csv
gsutil cp betterMid.csv gs://justinminsk_bucket/midterm/betterMid.csv
gsutil cp MidtermA.py gs://justinminsk_bucket/midterm/MidtermA.py