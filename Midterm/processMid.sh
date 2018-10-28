#!/bin/bash

gsutil cp -r gs://justinminsk_bucket/midterm/Aresult .

echo label,sent,received,count > result.csv
cat Aresult/part* >> result.csv

rm -r Aresult
gsutil rm -r gs://justinminsk_bucket/midterm/Aresult