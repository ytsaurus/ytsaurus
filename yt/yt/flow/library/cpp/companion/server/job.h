#pragma once

#include "public.h"

#include "state_store.h"

#include <yt/yt/flow/library/cpp/common/process_function.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/companion/companion_model.h>
#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include "runtime_context.h"

#include <yt/yt/core/actions/public.h>

namespace NYT::NFlow::NCompanionServer {

////////////////////////////////////////////////////////////////////////////////

//! Builds stream specs from the wire representation.
TStreamSpecsPtr BuildStreamSpecs(
    const google::protobuf::RepeatedPtrField<NProto::NCompanion::TStream>& protoStreams);

////////////////////////////////////////////////////////////////////////////////

//! Memoizes parsed stream specs by (stream id, spec id): source computations
//! resend the same per-batch override every epoch, and reusing the parsed spec
//! keeps schema pointers stable so the payload converter cache (keyed by
//! schema pointer) can hit instead of growing by one entry per epoch.
//! NB: Override spec ids are positional, renumbered from zero every batch, so
//! they carry no schema version; a hit is validated against the raw schema
//! bytes and re-parsed on mismatch (source schemas are data-derived and may
//! change under a live job).
class TStreamSpecCache
{
public:
    TStreamSpecsPtr Resolve(
        const google::protobuf::RepeatedPtrField<NProto::NCompanion::TStream>& protoStreams);

private:
    struct TEntry
    {
        TString SchemaBytes;
        TStreamSpecPtr Spec;
    };

    THashMap<std::pair<TStreamId, TStreamSpecId>, TEntry> Specs_;
};

////////////////////////////////////////////////////////////////////////////////

//! Companion-side view of a worker job: the parsed specs and stream registry
//! a batch of this job is processed against.
class TJob
    : public TRefCounted
{
public:
    TJob(
        TJobId jobId,
        TComputationId computationId,
        const NProto::NCompanion::TJobInfo& jobInfo,
        TResourceStorePtr resourceStore = nullptr);

    const TJobId& GetJobId() const;
    const TComputationId& GetComputationId() const;

    //! Exact direct and transitive companion resources required by the job.
    const std::vector<NCompanion::TCompanionResourceInstanceReference>& GetCompanionResources() const;

    const TComputationSpecPtr& GetSpec() const;
    const TDynamicComputationSpecPtr& GetDynamicSpec() const;
    const TStreamSpecsPtr& GetStreamSpecs() const;

    //! Internal state names come from the transform shim parameters
    //! (|internal_states| inside the computation's parameters map).
    const THashSet<std::string>& GetInternalStateNames() const;
    //! Keys of |external_state_managers| from the static spec.
    const THashSet<std::string>& GetExternalStateNames() const;
    //! Keys of |external_state_joiners| from the static spec.
    const THashSet<std::string>& GetJoinedStateNames() const;

    //! Runs one epoch batch through the hosted process function and fills the
    //! response data; the function is instantiated from the registry and
    //! initialized on first use. Returns false without processing when a
    //! required companion resource no longer matches its initialized instance
    //! (an in-band retryable condition). Not thread-safe: the companion
    //! service runs batches of one job id on the registry's per-job invoker.
    [[nodiscard]] bool ProcessBatch(
        const NProto::NCompanion::TReqProcessBatch& request,
        NProto::NCompanion::TResponseData* data);

private:
    const TJobId JobId_;
    const TComputationId ComputationId_;
    const TResourceStorePtr ResourceStore_;

    TComputationSpecPtr Spec_;
    TDynamicComputationSpecPtr DynamicSpec_;
    TStreamSpecsPtr StreamSpecs_;
    std::vector<NCompanion::TCompanionResourceInstanceReference> CompanionResources_;

    THashSet<std::string> InternalStateNames_;
    THashSet<std::string> ExternalStateNames_;
    THashSet<std::string> JoinedStateNames_;

    //! Per-job converter cache (mirrors the in-process worker's per-job
    //! scoping): entries are keyed by schema pointers and pinned for the
    //! cache's lifetime, so a process-global cache would grow forever.
    IPayloadConverterCachePtr ConverterCache_;
    //! Memoizes per-batch stream overrides so their schema pointers repeat.
    TStreamSpecCache OverrideSpecCache_;

    bool Initialized_ = false;
    TCompanionStateStorePtr StateStore_;
    IBatchProcessFunctionPtr BatchFunction_;
    //! Per-job runtime context, reused for batches without a stream override.
    TCompanionRuntimeContextPtr RuntimeContext_;

    [[nodiscard]] bool EnsureInitialized();
    std::optional<THashMap<TResourceId, IResourcePtr>> AcquireRequiredResources() const;
};

DEFINE_REFCOUNTED_TYPE(TJob);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanionServer
