#!/bin/bash
START=$1
END=$2
OUTPUT_PATH=$3

if [[ "$START" == "" ]]; then echo "START is required."; echo "Usage: amp_sxport.sh START END OUTPUT_PATH"; exit 1; fi
if [[ "$END" == "" ]]; then echo "END is required."; echo "Usage: amp_sxport.sh START END OUTPUT_PATH"; exit 1; fi
if [[ "$OUTPUT_PATH" == "" ]]; then echo "OUTPUT_PATH is required."; echo "Usage: amp_sxport.sh START END OUTPUT_PATH"; exit 1; fi

if [[ "$AMP_KEY" == "" ]]; then echo "AMP_KEY is not set."; exit 1; fi
if [[ "$AMP_SECRET" == "" ]]; then echo "AMP_SECRET is not set."; exit 1; fi


URL="https://amplitude.com/api/2/export?start=${START}&end=${END}"
EXPORTED_FILE=../amp_tmp_files/${START}-${END}.zip

mkdir -p ../amp_tmp_files/extracted


curl -silent "${URL}" -u "${AMP_KEY}:${AMP_SECRET}" --output ${EXPORTED_FILE}
unzip -q -o ${EXPORTED_FILE} -d ../amp_tmp_files/extracted

for GZ_FILE in ../amp_tmp_files/extracted/*/*.json.gz; do gzip -dfq $GZ_FILE; done


#for JSON_FILE in amp_tmp_files/extracted/*/*.json; do cat $JSON_FILE >> ${OUTPUT_PATH}; rm $JSON_FILE; done
#rm $EXPORTED_FILE
#rm -rf amp_tmp_files
echo ${OUTPUT_PATH}