#!/bin/bash
NAMESPACE=$1
PLATFORM_USER=$2
PLATFORM_PASS=$3
PLATFORM_SECRET=$4
OUTPUT_DIR=$5

#Some date info
SHORT_DATE=$(date +%Y-%m-%d)
job_start=$(date +%s)


#YEAR_FROM=$6
#MONTH_FROM=$7
#DAY_FROM=$8

#YEAR_TO=$9          
#MONTH_TO=$10
#DAY_TO=$11

#Gets the previous day for the vars
YEAR_FROM=$(date  --date="yesterday" +"%Y")
MONTH_FROM=$(date  --date="yesterday" +"%m")
DAY_FROM=$(date  --date="yesterday" +"%d")

YEAR_TO=$(date  +"%Y")
MONTH_TO=$(date  +"%m")
DAY_TO=$(date  +"%d")

#Testing dates - THIS should be commented out when actually using
YEAR_FROM='2022'
MONTH_FROM='09'
DAY_FROM='04'
#
YEAR_TO='2022'
MONTH_TO='09'
DAY_TO='06'


OUTPUT_FILENAME="${NAMESPACE}_${YEAR_FROM}-${MONTH_FROM}-${DAY_FROM}_${YEAR_TO}-${MONTH_TO}-${DAY_TO}.csv"
TEMP_FOLDER_NAME="${NAMESPACE}_${YEAR_FROM}-${MONTH_FROM}-${DAY_FROM}_${YEAR_TO}-${MONTH_TO}-${DAY_TO}"

echo "Downloading data in date range:"
echo "${YEAR_FROM}-${MONTH_FROM}-${DAY_FROM}_${YEAR_TO}-${MONTH_TO}-${DAY_TO}"

DATE_FROM="${YEAR_FROM}${MONTH_FROM}${DAY_FROM}000000"
DATE_TO="${YEAR_TO}${MONTH_TO}${DAY_TO}000000"

PLATFORM_URL="https://$NAMESPACE.platform.barter4things.com"

# Authenticate with platform
PLATFORM_AUTH=$(echo -n $PLATFORM_USER:$PLATFORM_PASS | base64)
PLATFORM_TOKEN=$(curl -s --location --request POST "$PLATFORM_URL/api/user/tokens" \
    --header "Authorization: Basic $PLATFORM_AUTH" | \
jq -r '.token')

# Get list of organisations from PROG, removing default and empty organisations

#Bit of hardcoding for SWT, TODO should find a way to do this better

if [[ "${NAMESPACE}" == "swt" ]]; then
    ORGANISATIONS=($(curl -s --location --request GET "$PLATFORM_URL/api/schemas/organisations" \
        --header "Authorization: Bearer $PLATFORM_TOKEN" | \
    jq -cr '.organisations | map(. | select(.id != 1)) | map(. | select(.groups | length > 0)) | map(.companyname) | @sh' | tr -d \'))
else
    ORGANISATIONS=($(curl -s --location --request GET "$PLATFORM_URL/api/schemas/organisations" \
            --header "Authorization: Bearer $PLATFORM_TOKEN" | \
    jq -cr '.organisations | map(. | select(.id != 0)) | map(. | select(.groups | length > 0)) | map(.companyname) | @sh' | tr -d \'))
fi
# Setup environment
kubectl config use-context do-lon1-b4t-k8s-prod-3

# Get celery pod name
CELERY_POD=$(kubectl get pods -n=$NAMESPACE --no-headers=true -o custom-columns=name:.metadata.name | grep -m 1 celery)


for org in "${ORGANISATIONS[@]}"
do
    start=`date +%s`
    echo -n "Fetching $org"
    
    # Delete old data
    kubectl exec -n=$NAMESPACE $CELERY_POD -- rm -rf /mnt/nfs/smartmeter/
    
    # Trigger celery task
    curl -s -H "Content-Type: application/json" -H "X-Client-Secret: $PLATFORM_SECRET" "https://$NAMESPACE.platform.barter4things.com//smartmeter/device/logs?datefrom=$DATE_FROM&dateto=$DATE_TO&organisation=$org" #> /dev/null
    #curl -s -H "Content-Type: application/json" "localhost:8080//smartmeter/device/logs?datefrom=$DATE_FROM&organisation=$org" #> /dev/null

    # Poll celery pod for existence of export file
    code=-1
    while [[ $code -ne 0 ]]; do
        sleep 5
        
        kubectl exec -n=$NAMESPACE $CELERY_POD -- [ -f "/mnt/nfs/smartmeter/smartmeter_all.csv" ] > /dev/null 2>&1
        code=$?

        if [[ $code -ne 0 ]]; then
            echo -n "."
        fi
    done
    
    # Download result
    kubectl cp $NAMESPACE/$CELERY_POD:/mnt/nfs/smartmeter/smartmeter_all.csv ./$TEMP_FOLDER_NAME/$org.csv > /dev/null 2>&1
    
    end=`date +%s`
    runtime=$((end-start))
    echo -e "\nFetched in $runtime seconds\n"
done

if [ ! -d ./sh/finalData/$NAMESPACE ]; then
  mkdir -p ./sh/finalData/$NAMESPACE;
fi
# Merge all CSV files
awk '(NR == 1) || (FNR > 1)' ./$TEMP_FOLDER_NAME/*.csv > ./sh/finalData/$NAMESPACE/$OUTPUT_FILENAME

#Change the header
sed -i '1s/.*/DeviceID,Timestamp,Epoch,rawData,msgType,msgSeq,readings,index,FLOW1,FLOW2,FLOW3,FLOW4,FLOW5,FLOW6,FLOW7,FLOW8,xtime,MAXFLOW,MINFLOW/' ./sh/finalData/$NAMESPACE/$OUTPUT_FILENAME

#Append the total data file
awk '(NR == 1) || (FNR > 1)' ./sh/finalData/$NAMESPACE/*.csv > $OUTPUT_DIR/$NAMESPACE-collectedData.csv

# Clean up
rm -rf ./$TEMP_FOLDER_NAME

job_end=$(date +%s)
job_runtime=$((job_end-job_start))

echo -e "\nMessage history for ${#ORGANISATIONS[@]} organisation(s) fetched in $job_runtime seconds\nOutput written to $OUTPUT_FILENAME"
