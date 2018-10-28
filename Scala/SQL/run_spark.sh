#!/bin/bash
gcloud dataproc jobs submit spark --cluster my-cluster2  --jar target/scala-2.11/sql_2.11-0.1.jar
