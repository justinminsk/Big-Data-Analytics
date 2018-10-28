#!/bin/bash

gsutil cp -r gs://justinminsk_bucket/classwork/scalaResults .

echo year,month,date,name,hr > result.csv
cat scalaResults/part* >> result.csv

rm -r scalaResults
gsutil rm -r gs://justinminsk_bucket/classwork/scalaResults/