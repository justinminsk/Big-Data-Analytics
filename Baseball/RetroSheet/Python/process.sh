#!/bin/bash

gsutil cp -r gs://justinminsk_bucket/classwork/output .

echo year,month,date,name,hr > result.csv
cat output/part* >> result.csv

rm -r output
gsutil rm -r gs://justinminsk_bucket/classwork/output/