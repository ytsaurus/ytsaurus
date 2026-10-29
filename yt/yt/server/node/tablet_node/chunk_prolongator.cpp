#include "chunk_prolongator.h"

#include <yt/yt/server/lib/tablet_node/private.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/invoker_util.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TChunkProlongator
    : public IChunkProlongator
{
public:
    THashSet<TChunkId>& ProlongedChunks() override
    {
        return ProlongedChunks_;
    }

    const THashSet<TChunkId>& ProlongedChunks() const override
    {
        return ProlongedChunks_;
    }

    bool IsChunkProlongationRequired(TChunkId chunkId) const override
    {
        return SnapshotChunksRefCount_.contains(chunkId) && !ProlongedChunks_.contains(chunkId);
    }

    void RegisterSnapshotChunks(const std::vector<TChunkId>& chunkIds) override
    {
        for (auto chunkId : chunkIds) {
            ++SnapshotChunksRefCount_[chunkId];
        }
    }

    void UnregisterSnapshotChunks(TRange<TChunkId> chunkIds) override
    {
        for (auto chunkId : chunkIds) {
            auto it = SnapshotChunksRefCount_.find(chunkId);
            if (it == SnapshotChunksRefCount_.end()) {
                YT_TLOG_ALERT("Chunk is missing on deregistration from prolongator")
                    .With("ChunkId", chunkId);

                continue;
            }

            if (--it->second == 0) {
                SnapshotChunksRefCount_.erase(it);
                InsertChunkIntoDeprolongationQueue(chunkId);
            }
        }
    }

    void InsertChunkIntoDeprolongationQueue(TChunkId chunkId) override
    {
        if (ProlongedChunks_.contains(chunkId)) {
            DeprolongationQueue_.push(chunkId);
        }
    }

    std::vector<TChunkId> GetNotReferencedProlongedChunkIds(int maxCount) override
    {
        std::vector<TChunkId> result;
        while (!DeprolongationQueue_.empty() && std::ssize(result) < maxCount) {
            auto chunkId = DeprolongationQueue_.front();
            DeprolongationQueue_.pop();
            if (ProlongedChunks_.contains(chunkId) && !SnapshotChunksRefCount_.contains(chunkId)) {
                result.push_back(chunkId);
            }
        }
        return result;
    }

    void Save(TSaveContext& context) const override
    {
        NYT::Save(context, ProlongedChunks_);
    }

    void Load(TLoadContext& context) override
    {
        NYT::Load(context, ProlongedChunks_);

        for (const auto& chunkId : ProlongedChunks_) {
            DeprolongationQueue_.push(chunkId);
        }
    }

    void ToProto(google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>* mutableChunkIds) const override
    {
        NYT::ToProto(mutableChunkIds, ProlongedChunks_);
    }

    void FromProto(const google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>& prolongedChunkIds) override
    {
        NYT::FromProto(&ProlongedChunks_, prolongedChunkIds);

        for (const auto& chunkId : ProlongedChunks_) {
            DeprolongationQueue_.push(chunkId);
        }
    }

private:
    // Persistent, mirrors master's state.
    THashSet<TChunkId> ProlongedChunks_;

    // Transient: reference counts from live snapshots.
    THashMap<TChunkId, int> SnapshotChunksRefCount_;

    // Transient: chunk IDs pending deprolongation.
    std::queue<TChunkId> DeprolongationQueue_;
};

////////////////////////////////////////////////////////////////////////////////

IChunkProlongatorPtr CreateChunkProlongator()
{
    return New<TChunkProlongator>();
}

////////////////////////////////////////////////////////////////////////////////

TChunkProlongationGuard::TChunkProlongationGuard(
    IChunkProlongatorPtr prolongator,
    IInvokerPtr epochAutomatonInvoker,
    std::vector<TChunkId> snapshotChunkIds)
    : Prolongator_(prolongator)
    , EpochAutomatonInvoker_(std::move(epochAutomatonInvoker))
    , SnapshotChunkIds_(std::move(snapshotChunkIds))
{
    prolongator->RegisterSnapshotChunks(SnapshotChunkIds_);
}

TChunkProlongationGuard::~TChunkProlongationGuard()
{
    if (SnapshotChunkIds_.empty()) {
        return;
    }

    auto prolongator = Prolongator_.Lock();
    if (!prolongator) {
        return;
    }

    EpochAutomatonInvoker_->Invoke(BIND(
        [prolongator = std::move(prolongator), chunkIds = std::move(SnapshotChunkIds_)] {
            prolongator->UnregisterSnapshotChunks(chunkIds);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
