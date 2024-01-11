#pragma once

#include "store.h"
#include "public.h"

#include <yt/yt/server/lib/lsm/partition.h>

namespace NYT::NLsm::NTesting {

////////////////////////////////////////////////////////////////////////////////

class TPartition
    : public NLsm::TPartition
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::unique_ptr<TStore>>, Stores);
    DEFINE_BYVAL_RW_PROPERTY(TTablet*, Tablet);

public:
    TPartition()
    {
        static int Counter = 1;
        Id_.Parts32[0] = Counter++;
        Id_.Parts32[1] = 705;
        Id_.Parts32[2] = EpochIndex;
        Id_.Parts32[3] = 1;
    }

    explicit TPartition(TTablet* tablet)
        : TPartition()
    {
        SetTablet(tablet);
    }

    TPartition(const TPartition&) = delete;
    TPartition(TPartition&&) = delete;
    TPartition& operator=(const TPartition&) = delete;
    TPartition& operator=(TPartition&&) = delete;

    std::unique_ptr<NLsm::TPartition> ToLsmPartition(NLsm::TTablet* tablet)
    {
        auto& lsmStores = NLsm::TPartition::Stores_;
        for (auto& store : Stores_) {
            lsmStores.push_back(store->ToLsmStore(tablet));
        }

        NLsm::TPartition::Tablet_ = tablet;

        // NB: We return unique_ptr(this) but ensure that owner will
        // call .reset() on it before destructing.
        return std::unique_ptr<NLsm::TPartition>(this);
    }

    void ResetLsmState()
    {
        auto& lsmStores = NLsm::TPartition::Stores_;
        for (auto& store : lsmStores) {
            if (store->GetType() != EStoreType::SortedDynamic) {
                store.release();
            }
        }
        lsmStores.clear();
    }

    void AddStore(std::unique_ptr<TStore> store)
    {
        CompressedDataSize_ += store->GetCompressedDataSize();
        UncompressedDataSize_ += store->GetUncompressedDataSize();
        Stores_.push_back(std::move(store));
    }

    void RemoveStore(TStoreId storeId)
    {
        auto* store = GetStore(storeId);
        CompressedDataSize_ -= store->GetCompressedDataSize();
        UncompressedDataSize_ -= store->GetUncompressedDataSize();

        auto it = Stores_.begin();
        while ((*it)->GetId() != storeId) {
            ++it;
        }
        Stores_.erase(it);
    }

    TStore* FindStore(TStoreId storeId) const
    {
        for (auto& store : Stores_) {
            if (store->GetId() == storeId) {
                return store.get();
            }
        }
        return nullptr;
    }

    TStore* GetStore(TStoreId storeId) const
    {
        auto* store = FindStore(storeId);
        YT_VERIFY(store);
        return store;
    }

    void SetState(EPartitionState state);

    void Persist(const TStreamPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm::NTesting
