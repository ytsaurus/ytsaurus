#!/bin/bash

#ALGOLIA_APP_ID="app-id"
#ALGOLIA_API_KEY="admin-api-key"
MIN_INDEX_COUNT=10      # Минимальное количество индексов, которые нужно оставить
COUNT_TO_DELETE=1       # Сколько удалить за один запуск

# Получить список индексов
response=$(curl -s -H "X-Algolia-API-Key: $ALGOLIA_API_KEY" \
    -H "X-Algolia-Application-Id: $ALGOLIA_APP_ID" \
    "https://${ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes")

# Посчитать количество индексов
count=$(echo "$response" | jq '.items | length')

echo "Всего индексов: $count"

if [ "$count" -le "$MIN_INDEX_COUNT" ]; then
  echo "Удаление не требуется, индексов $count (минимум: $MIN_INDEX_COUNT)."
  exit 0
fi

# Отсортировать по updatedAt (самые старые – первые)
indices=$(echo "$response" | jq -c '.items | sort_by(.updatedAt)[] | {name,updatedAt}')

# Получить N самых старых
to_delete=$(echo "$indices" | head -n "$COUNT_TO_DELETE" | jq -r '.name')

echo "Удаляю индексы: $to_delete"

for idx in $to_delete; do
  echo "Удаляю индекс: $idx"
  curl -s -X DELETE \
       -H "X-Algolia-API-Key: $ALGOLIA_API_KEY" \
       -H "X-Algolia-Application-Id: $ALGOLIA_APP_ID" \
       "https://${ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/$idx"
done
