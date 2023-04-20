TMP_FOLDER="patch_tmp"
PROPERTIES_FILE="sidecar-config/metrics.properties"
INPUT_FILE=$1
OUTPUT_FILE=patched_$1
OPTION="*.sink.solomon.reporter_enabled"

mkdir $TMP_FOLDER

(
cd $TMP_FOLDER || exit
unzip ../$INPUT_FILE $PROPERTIES_FILE
if grep -xq "$OPTION=true" $PROPERTIES_FILE
then
  echo "Disabling solomon in $PROPERTIES_FILE"
  sed -i '' "s/$OPTION=true/$OPTION=false/g" $PROPERTIES_FILE &&
    cp ../$INPUT_FILE ../$OUTPUT_FILE &&
    zip ../$OUTPUT_FILE $PROPERTIES_FILE &&
    echo "Updated launcher file $OUTPUT_FILE created successfully"
else
  echo "Config file doesn't contain '$OPTION' line"
fi
)

rm -r $TMP_FOLDER
