#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IChunkProlongator
    : public TRefCounted
{
    virtual THashSet<NChunkClient::TChunkId>& ProlongedChunks() = 0;
    virtual const THashSet<NChunkClient::TChunkId>& ProlongedChunks() const = 0;

    virtual bool IsChunkProlongationRequired(NChunkClient::TChunkId chunkId) const = 0;
    virtual void RegisterSnapshotChunks(const std::vector<NChunkClient::TChunkId>& chunkIds) = 0;
    virtual void UnregisterSnapshotChunks(TRange<NChunkClient::TChunkId> chunkIds) = 0;
    virtual void InsertChunkIntoDeprolongationQueue(NChunkClient::TChunkId chunkId) = 0;

    virtual std::vector<NChunkClient::TChunkId> GetNotReferencedProlongedChunkIds(int maxCount) = 0;

    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;

    virtual void ToProto(google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>* mutableChunkIds) const = 0;
    virtual void FromProto(const google::protobuf::RepeatedPtrField<NYT::NProto::TGuid>& prolongedChunkIds) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkProlongator)

IChunkProlongatorPtr CreateChunkProlongator();

////////////////////////////////////////////////////////////////////////////////

class TChunkProlongationGuard
{
public:
    TChunkProlongationGuard(
        IChunkProlongatorPtr prolongator,
        IInvokerPtr epochAutomatonInvoker,
        std::vector<NChunkClient::TChunkId> snapshotChunkIds);

    ~TChunkProlongationGuard();

    TChunkProlongationGuard(const TChunkProlongationGuard&) = delete;
    TChunkProlongationGuard& operator=(const TChunkProlongationGuard&) = delete;

    TChunkProlongationGuard(TChunkProlongationGuard&&) = default;
    TChunkProlongationGuard& operator=(TChunkProlongationGuard&&) = default;

private:
    TWeakPtr<IChunkProlongator> Prolongator_;
    IInvokerPtr EpochAutomatonInvoker_;
    std::vector<NChunkClient::TChunkId> SnapshotChunkIds_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
