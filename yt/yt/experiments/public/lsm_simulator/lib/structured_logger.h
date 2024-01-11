#pragma once

#include "tablet.h"
#include "store.h"
#include "partition.h"
#include "public.h"

#include <library/cpp/json/writer/json_value.h>

namespace NYT::NLsm::NTesting {

using NJson::TJsonValue;
using NJson::TJsonMap;
using NJson::TJsonArray;

////////////////////////////////////////////////////////////////////////////////

class TStructuredLogger
    : public TRefCounted
{
public:
    void LogEvent(const NJson::TJsonValue& value);

    void OnTabletFullHeartbeat(TTablet* tablet);

    TJsonValue OnPartitionFullHeartbeat(TPartition* partition);

    TJsonValue OnStoreFullHeartbeat(TStore* store);

    void OnTabletStoresUpdateCommitted(
        const std::vector<TStore*> addedStores,
        const std::vector<TStoreId> removedStoreIds,
        TStringBuf updateReason,
        std::optional<EStoreCompactionReason> compactionReason = std::nullopt);

    void OnPartitionStateChanged(TPartition* partition);

    void OnStoreStateChanged(TStore* store);

    void OnStoreCompactionStateChanged(TStore* store);

    void OnStorePreloadStateChanged(TStore* store);

    void OnStoreFlushStateChanged(TStore* store);

    // TODO: different from node structured logger.
    void OnPartitionSplit(TTablet* tablet, TPartitionId oldPartitionId, int firstPartitionIndex, int splitFactor);

    void OnPartitionsMerged(const std::vector<TPartitionId>& oldPartitionIds, const TPartition* newPartition);

    void OnTabletUnlocked(const std::vector<std::unique_ptr<TStore>>& stores);

    std::vector<TJsonValue> Consume();

private:
    std::vector<NJson::TJsonValue> Events_;

    template <class T>
    void InsertValue(TJsonValue& map, TStringBuf key, T&& value)
    {
        if constexpr (std::is_same_v<std::decay_t<T>, TNativeKey>) {
            map.InsertValue(key, KeyToJson(value));
        } else if constexpr (requires {map.InsertValue(key, value); }) {
            map.InsertValue(key, std::forward<T>(value));
        } else {
            map.InsertValue(key, ToString(value));
        }
    }

    TJsonValue KeyToJson(const TNativeKey& key)
    {
        TJsonValue array(NJson::EJsonValueType::JSON_ARRAY);

        for (auto value : key) {
            switch (value.Type) {
                case NTableClient::EValueType::Max: {
                    TJsonMap map{
                        {"$attributes", {TJsonMap{{"type", "max"}}}}
                    };
                    array.AppendValue(map);
                    break;
                }

                case NTableClient::EValueType::Min: {
                    TJsonMap map{
                        {"$attributes", {TJsonMap{{"type", "min"}}}}
                    };
                    array.AppendValue(map);
                    break;
                }

                case NTableClient::EValueType::Int64:
                    array.AppendValue(value.Data.Int64);
                    break;

                default:
                    YT_ABORT();
            }
        }

        return array;
    }

    static TJsonValue GetEventMap(TStringBuf eventType)
    {
        TJsonValue eventMap(NJson::EJsonValueType::JSON_MAP);
        eventMap.InsertValue("entry_type", "event");
        eventMap.InsertValue("event_type", eventType);
        return eventMap;
    }
};

DEFINE_REFCOUNTED_TYPE(TStructuredLogger)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
