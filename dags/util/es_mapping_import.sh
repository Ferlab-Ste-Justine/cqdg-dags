#!/bin/bash

curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o $HOME/minio-binaries/mc

chmod +x $HOME/minio-binaries/mc
export PATH=$PATH:$HOME/minio-binaries/

echo Setting MC alias to this minio: $AWS_ENDPOINT
mc alias set myminio $1 $2 $3 $4

mkdir templates

echo Downloading templates ...
wget  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_study_centric.json -O ./templates/template_study_centric.json
wget  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_file_centric.json -O ./templates/template_file_centric.json
wget  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_participant_centric.json -O ./templates/template_participant_centric.json
wget  https://raw.githubusercontent.com/Ferlab-Ste-Justine/etl-cqdg-portal/master/index-task/src/main/resources/templates/template_biospecimen_centric.json -O ./templates/template_biospecimen_centric.json

echo Copy templates ...
mc cp ./templates/template_study_centric.json myminio/cqdg-"$4"-app-clinical-data-service/templates/template_study_centric.json
mc cp ./templates/template_file_centric.json myminio/cqdg-"$4"-app-clinical-data-service/templates/template_file_centric.json
mc cp ./templates/template_participant_centric.json myminio/cqdg-"$4"-app-clinical-data-service/templates/template_participant_centric.json
mc cp ./templates/template_biospecimen_centric.json myminio/cqdg-"$4"-app-clinical-data-service/templates/template_biospecimen_centric.json
