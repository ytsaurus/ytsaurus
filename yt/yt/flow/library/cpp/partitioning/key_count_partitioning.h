#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <span>

namespace NYT::NFlow::NPartitioning {

////////////////////////////////////////////////////////////////////////////////

struct TKeyCountPartitioningState
    : public NYTree::TYsonStruct
{
    THashMap<TKey, TInstant> ObservedKeys;
    TInstant ExpiredBefore;

    REGISTER_YSON_STRUCT(TKeyCountPartitioningState);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyCountPartitioningState);

i64 CountExecutableKeyCountOwners(const TFlowViewPtr& flowView, const TComputationId& computationId);

void IngestObservedKeys(
    const TKeyCountPartitioningStatePtr& state,
    const THashMap<TKey, TInstant>& keys);

void ExpireKeyCatalogue(
    const TKeyCountPartitioningStatePtr& state,
    TInstant cutoff);

std::vector<TKeyRange> BuildKeyCountRanges(
    std::span<const TKey> sortedKeys,
    const TKeyRange& range,
    i64 maxKeys);

class TKeyCountPartitioning
{
public:
    struct TResult
    {
        std::vector<TKeyRange> Ranges;
        THashSet<TPartitionId> RetainedPartitions;
        std::string Reason;
    };

    TKeyCountPartitioning(TKeyCountPartitioningStatePtr state, IStatusErrorStatePtr errorState);

    void BeginIteration(const TFlowViewPtr& committedFlowView);
    void Commit();
    TErrorOr<TResult> Partition(
        const TComputationId& computationId,
        const TPartitioningSpecPtr& spec,
        const TDynamicPartitioningSpecPtr& dynamicSpec,
        const THashMap<TPartitionId, TKeyRange>& ranges,
        const TFlowViewPtr& flowView,
        bool firstKeyIsUint);

private:
    const TKeyCountPartitioningStatePtr State_;
    const IStatusErrorStatePtr ErrorState_;
    std::optional<TInstant> ActiveSince_;
    TInstant LastRebalanceAt_;
    TInstant PendingRebalanceAt_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NPartitioning
