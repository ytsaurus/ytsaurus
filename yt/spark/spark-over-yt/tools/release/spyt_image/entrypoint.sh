if [[ -z "$PROXY" ]]; then
  echo "Error: PROXY env variable is not seе. Please, use -e PROXY=<proxy> for 'docker run' command"
  exit 1
fi

echo "EXTRA_CONFIG_GENERATOR_OPTIONS = $EXTRA_CONFIG_GENERATOR_OPTIONS"
echo "EXTRA_PUBLISH_CLUSTER_OPTIONS = $EXTRA_PUBLISH_CLUSTER_OPTIONS"

python3.7 /scripts/config_generator.py /data --proxy $PROXY $EXTRA_CONFIG_GENERATOR_OPTIONS
python3.7 /scripts/publish_cluster.py /data $PROXY $EXTRA_PUBLISH_CLUSTER_OPTIONS
