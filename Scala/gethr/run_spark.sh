#!/bin/bash
gcloud dataproc jobs submit spark --cluster cluster-b392  --jar target/scala-2.11/gethr_2.11-0.1.jar
