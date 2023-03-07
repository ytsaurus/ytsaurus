#pragma once

#include "public.h"

#include <yt/client/api/client.h>
#include <yt/client/api/operation_archive_schema.h>

#include <yt/client/object_client/public.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TListOperationsCountingFilter
{
public:
    explicit TListOperationsCountingFilter(const TListOperationsOptions& options);

    bool Filter(
        const std::optional<std::vector<TString>>& pools,
        TStringBuf user,
        NScheduler::EOperationState state,
        NScheduler::EOperationType type,
        i64 count);
    bool FilterByFailedJobs(bool hasFailedJobs, i64 count);

public:
    THashMap<TString, i64> PoolCounts;
    THashMap<TString, i64> UserCounts;
    TEnumIndexedVector<NScheduler::EOperationState, i64> StateCounts;
    TEnumIndexedVector<NScheduler::EOperationType, i64> TypeCounts;
    i64 FailedJobsCount = 0;

private:
     const TListOperationsOptions& Options_;
};

class TListOperationsFilter
{
public:
    struct TBriefProgress
    {
        bool HasFailedJobs;
        TInstant BuildTime;
    };

    struct TLightOperation
    {
    public:
        [[nodiscard]] NObjectClient::TOperationId GetId() const;
        void UpdateBriefProgress(TStringBuf briefProgressYson);
        void SetYson(TString yson);

    private:
        NObjectClient::TOperationId Id_;
        TInstant StartTime_;
        TBriefProgress BriefProgress_;
        TString Yson_;

    private:
        friend class TFilteringConsumer;
        friend class TListOperationsFilter;
    };

public:
    // NB: Each element of |operations| vector are assumed to be
    // YSON lists containing operations in format "id with attributes"
    // (as returned from Cypress "list" command).
    TListOperationsFilter(
        const std::vector<NYson::TYsonString>& operations,
        TListOperationsCountingFilter& countingFilter,
        const TListOperationsOptions& options);

    template <typename TFunction>
    void ForEachOperationImmutable(TFunction function) const;

    template <typename TFunction>
    void ForEachOperationMutable(TFunction function);

    [[nodiscard]] std::vector<TOperation> BuildOperations(const THashSet<TString>& attributes) const;

    [[nodiscard]] i64 GetCount() const;

    // Confirms that |brief_progress| field is relevant and filtration by it can be applied.
    void OnBriefProgressFinished();

private:
    TListOperationsCountingFilter& CountingFilter_;
    const TListOperationsOptions& Options_;
    std::vector<TLightOperation> LightOperations_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

#define LIST_OPERATIONS_INL_H
#include "list_operations-inl.h"
#undef LIST_OPERATIONS_INL_H
