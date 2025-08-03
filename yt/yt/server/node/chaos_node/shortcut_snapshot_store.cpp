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
    void UpdateShortcut(TChaosObjectId chaosObjectId, TShortcutSnapshot snapshot) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto* bucket = GetBucket(chaosObjectId);
        auto guard = WriterGuard(bucket->Lock);
        bucket->ShortcutSnapshots[chaosObjectId] = snapshot;
    }

    void RemoveShortcut(TChaosObjectId chaosObjectId) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto* bucket = GetBucket(chaosObjectId);
        auto guard = WriterGuard(bucket->Lock);
        bucket->ShortcutSnapshots.erase(chaosObjectId);
    }

    std::optional<TShortcutSnapshot> FindShortcut(TChaosObjectId chaosObjectId) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto* bucket = GetBucket(chaosObjectId);
        {
            auto guard = ReaderGuard(bucket->Lock);
            if (auto it = bucket->ShortcutSnapshots.find(chaosObjectId)) {
                return it->second;
            }
        }
        return {};
    }

    TShortcutSnapshot GetShortcutOrThrow(TChaosObjectId chaosObjectId) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto shortcut = FindShortcut(chaosObjectId);
        if (!shortcut) {
            THROW_ERROR_EXCEPTION("No shortcut for replication card %v",
                chaosObjectId);
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
        THashMap<TChaosObjectId, TShortcutSnapshot> ShortcutSnapshots;
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
    };

    std::vector<TBucket> Buckets_{ConcurrentHashBucketCount};

    TBucket* GetBucket(TChaosObjectId replicationCardId)
    {
        return &Buckets_[GetShardIndex<ConcurrentHashBucketCount>(replicationCardId)];
    }
};

////////////////////////////////////////////////////////////////////////////////

IShortcutSnapshotStorePtr CreateShortcutSnapshotStore()
{
    return New<TShortcutSnapshotStore>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
