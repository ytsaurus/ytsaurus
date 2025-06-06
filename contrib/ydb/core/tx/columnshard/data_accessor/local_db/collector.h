#pragma once
#include <contrib/ydb/core/tx/columnshard/data_accessor/abstract/collector.h>
#include <contrib/ydb/core/tx/columnshard/data_accessor/request.h>
#include <contrib/ydb/core/tx/columnshard/common/path_id.h>

#include <library/cpp/cache/cache.h>

namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB {

class TCollector: public IGranuleDataAccessor {
private:
    const NActors::TActorId TabletActorId;
    struct TMetadataSizeProvider {
        size_t operator()(const TPortionDataAccessor& data) {
            return data.GetMetadataSize();
        }
    };

    TLRUCache<ui64, TPortionDataAccessor, TNoopDelete, TMetadataSizeProvider> AccessorsCache;
    using TBase = IGranuleDataAccessor;
    virtual void DoAskData(THashMap<TInternalPathId, TPortionsByConsumer>&& portions,
        const std::shared_ptr<IAccessorCallback>& callback) override;
    virtual TDataCategorized DoAnalyzeData(const TPortionsByConsumer& portions) override;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) override;

public:
    TCollector(const TInternalPathId pathId, const ui64 maxSize, const NActors::TActorId& actorId)
        : TBase(pathId)
        , TabletActorId(actorId)
        , AccessorsCache(maxSize) {
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl::NLocalDB
