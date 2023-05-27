#include "shortcut_snapshot_store.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yt/string/guid.h>

namespace NYT::NChaosNode {

using namespace NChaosClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr int ConcurrentHashBucketCount = 256;

////////////////////////////////////////////////////////////////////////////////

class TShortcutSnapshotStore
    : public IShortcutSnapshotStore
{
public:
    void UpdateShortcut(TReplicationCardId replicationCardId, TShortcutSnapshot snaphsot) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto* bucket = GetBucket(replicationCardId);
        auto guard = WriterGuard(bucket->Lock);
        bucket->ShortcutSnapshots[replicationCardId] = snaphsot;
    }

    void RemoveShortcut(TReplicationCardId replicationCardId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto* bucket = GetBucket(replicationCardId);
        auto guard = WriterGuard(bucket->Lock);
        bucket->ShortcutSnapshots.erase(replicationCardId);
    }

    std::optional<TShortcutSnapshot> FindShortcut(TReplicationCardId replicationCardId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto* bucket = GetBucket(replicationCardId);
        {
            auto guard = ReaderGuard(bucket->Lock);
            if (auto it = bucket->ShortcutSnapshots.find(replicationCardId)) {
                return it->second;
            }
        }
        return {};
    }

    TShortcutSnapshot GetShortcutOrThrow(TReplicationCardId replicationCardId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto shortcut = FindShortcut(replicationCardId);
        if (!shortcut) {
            THROW_ERROR_EXCEPTION("No shortcut for replication card %v",
                replicationCardId);
        }
        return *shortcut;
    }

    void Clear() override
    {
        for (auto& bucket : Buckets_) {
            auto guard = WriterGuard(bucket.Lock);
            bucket.ShortcutSnapshots.clear();
        }
    }

private:
    struct TBucket
    {
        THashMap<TReplicationCardId, TShortcutSnapshot> ShortcutSnapshots;
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
    };

    std::vector<TBucket> Buckets_{ConcurrentHashBucketCount};

    TBucket* GetBucket(TReplicationCardId replicationCardId)
    {
        return &Buckets_[HashFromId(replicationCardId) % ConcurrentHashBucketCount];
    }
};

////////////////////////////////////////////////////////////////////////////////

IShortcutSnapshotStorePtr CreateShortcutSnapshotStore()
{
    return New<TShortcutSnapshotStore>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
