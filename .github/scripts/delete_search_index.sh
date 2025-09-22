#!/bin/bash

ALGOLIA_APP_ID=$1
ALGOLIA_API_KEY=$2
MIN_INDEX_COUNT=10      # Minimum number of indexes to keep.
COUNT_TO_DELETE=4       # Number of indexes to delete per run.

# Get list of indexes.
response=$(curl -s -H "X-Algolia-API-Key: $ALGOLIA_API_KEY" \
    -H "X-Algolia-Application-Id: $ALGOLIA_APP_ID" \
    "https://${ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes")

# Count the number of indexes.
count=$(echo "$response" | jq '.items | length')

echo "Total indices: $count"

if [ "$count" -le "$MIN_INDEX_COUNT" ]; then
  echo "Deletion not requiered, there are $count indices now (minimum is $MIN_INDEX_COUNT)."
  exit 0
fi

# Sort by updatedAt (oldest first).
indices=$(echo "$response" | jq -c '.items | sort_by(.updatedAt)[] | {name,updatedAt}')

# Get N oldest.
to_delete=$(echo "$indices" | head -n "$COUNT_TO_DELETE" | jq -r '.name')

echo "Indexes to delete: $to_delete"

for idx in $to_delete; do
  echo "Deleting index: $idx"
  curl -s -X DELETE \
       -H "X-Algolia-API-Key: $ALGOLIA_API_KEY" \
       -H "X-Algolia-Application-Id: $ALGOLIA_APP_ID" \
       "https://${ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/$idx"
done
