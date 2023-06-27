#!/bin/bash

curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o $HOME/minio-binaries/mc

chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries/


mc cp https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_study_centric.json https://s3.ops.cqdg.ferlab.bio/minio/cqdg-qa-app-clinical-data-service/templates/template_study_centric.json

mc ls

mc --help
