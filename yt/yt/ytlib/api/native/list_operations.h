#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/operation_archive_schema.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TListOperationsFilter)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EListOperationsCountingFilterType,
    (PoolTree)
    (Pool)
    (User)
    (OperationState)
    (OperationType)
    (WithFailedJobs)
);

////////////////////////////////////////////////////////////////////////////////

struct TCountingFilterAttributes {
    std::optional<THashMap<TString, TString>> PoolTreeToPool;
    std::optional<std::vector<TString>> Pools;
    TString User;
    NScheduler::EOperationState State = {};
    NScheduler::EOperationType Type = {};
    bool HasFailedJobs = false;
};

struct TPreliminaryCountingFilterResult {
    bool Passed = false;
    bool UseInCounter = false;
    std::optional<EListOperationsCountingFilterType> FailedFilterType;
};

////////////////////////////////////////////////////////////////////////////////

using TCountByPoolTree = THashMap<TString, i64>;
using TCountByPool = THashMap<TString, i64>;
using TCountByUser = THashMap<TString, i64>;
using TCountByType = TEnumIndexedVector<NScheduler::EOperationType, i64>;
using TCountByState = TEnumIndexedVector<NScheduler::EOperationState, i64>;

////////////////////////////////////////////////////////////////////////////////

class TListOperationsCountingFilter
{
public:
    DEFINE_BYREF_RW_PROPERTY(TCountByPoolTree, PoolTreeCounts);
    DEFINE_BYREF_RW_PROPERTY(TCountByPool, PoolCounts);
    DEFINE_BYREF_RW_PROPERTY(TCountByUser, UserCounts);
    DEFINE_BYREF_RW_PROPERTY(TCountByState, StateCounts);
    DEFINE_BYREF_RW_PROPERTY(TCountByType, TypeCounts);
    DEFINE_BYVAL_RW_PROPERTY(i64, FailedJobsCount);

public:
    TListOperationsCountingFilter() = default;

    explicit TListOperationsCountingFilter(const TListOperationsOptions& options);

    TPreliminaryCountingFilterResult PreliminaryFilter(const TCountingFilterAttributes& countingFilterAttributes);
    bool Filter(const TCountingFilterAttributes& countingFilterAttributes, i64 count);
    bool Filter(
        const TCountingFilterAttributes& countingFilterAttributes,
        const TPreliminaryCountingFilterResult& preliminaryCountingFilterResult,
        i64 count);

    void MergeFrom(const TListOperationsCountingFilter& otherFilter);

private:
    // NB: we have to use pointer instead of reference since
    // default constructor is needed in this class.
    const TListOperationsOptions* Options_ = nullptr;
};

class TListOperationsFilter
    : public TRefCounted
{
public:
    struct TBriefProgress
    {
        bool HasFailedJobs;
        TInstant BuildTime;
    };

    class TLightOperation
    {
    public:
        DEFINE_BYREF_RW_PROPERTY(TCountingFilterAttributes, FilterAttributes);
        DEFINE_BYREF_RW_PROPERTY(TPreliminaryCountingFilterResult, PreFilterResult);
        DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TOperationId, Id);
        DEFINE_BYVAL_RW_PROPERTY(TInstant, StartTime);
        DEFINE_BYREF_RW_PROPERTY(TString, Yson);
        DEFINE_BYREF_RW_PROPERTY(TBriefProgress, BriefProgress);

    public:
        void UpdateBriefProgress(TStringBuf briefProgressYson);

    private:
        friend class TFilteringConsumer;
    };

public:
    TListOperationsFilter(
        const TListOperationsOptions& options,
        const IInvokerPtr& invoker,
        const NLogging::TLogger& logger);

    // NB: Each element of |responses| vector is assumed to be
    // a YSON list containing operations in format "id with attributes"
    // (as returned from Cypress "list" command).
    void ParseResponses(std::vector<NYson::TYsonString> responses);

    template <typename TFunction>
    void ForEachOperationImmutable(TFunction function) const;

    template <typename TFunction>
    void ForEachOperationMutable(TFunction function);

    [[nodiscard]] std::vector<TOperation> BuildOperations(const THashSet<TString>& attributes) const;

    [[nodiscard]] i64 GetCount() const;

    // Confirms that |brief_progress| field is relevant and filtration by it can be applied.
    void OnBriefProgressFinished();

    const TListOperationsCountingFilter& GetCountingFilter() const;

private:
    // NB. TListOperationsFilter must own all its fields because it is used
    // in async context.
    const TListOperationsOptions Options_;
    TListOperationsCountingFilter CountingFilter_;
    const IInvokerPtr Invoker_;
    const NLogging::TLogger Logger;
    std::vector<TLightOperation> LightOperations_;

    struct TParseResult
    {
        std::vector<TLightOperation> Operations;
        TListOperationsCountingFilter CountingFilter;
    };

    TParseResult ParseOperationsYson(NYson::TYsonString operationsYson) const;
};

DEFINE_REFCOUNTED_TYPE(TListOperationsFilter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

#define LIST_OPERATIONS_INL_H
#include "list_operations-inl.h"
#undef LIST_OPERATIONS_INL_H
