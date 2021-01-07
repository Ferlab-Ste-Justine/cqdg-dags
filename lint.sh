#!/bin/bash
for FILENAME in $(pwd)/*; do
    if [ -d "$FILENAME" ] ; then
        su -c "pylint $FILENAME/*.py" airflow
    fi
done