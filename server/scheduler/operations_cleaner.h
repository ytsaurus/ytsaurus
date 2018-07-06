#pragma once

#include "public.h"
#include "operation.h"

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TArchiveOperationRequest
{
    TOperationId Id;
    TInstant StartTime;
    TInstant FinishTime;
    EOperationState State;
    TString AuthenticatedUser;
    EOperationType OperationType;
    NYson::TYsonString Progress;
    NYson::TYsonString BriefProgress;
    NYson::TYsonString Spec;
    NYson::TYsonString BriefSpec;
    NYson::TYsonString Result;
    NYson::TYsonString Events;
    NYson::TYsonString Alerts;
    TNullable<int> SlotIndex;
    // Archive version >= 17
    NYson::TYsonString FullSpec;
    NYson::TYsonString UnrecognizedSpec;
    // Archive version >= 22
    NYson::TYsonString RuntimeParameters;

    void InitializeFromOperation(const TOperationPtr& operation);

    static const std::vector<TString>& GetAttributeKeys();
    void InitializeFromAttributes(const NYTree::IAttributeDictionary& attributes);
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveOperationRequest
{
    TOperationId Id;
};

////////////////////////////////////////////////////////////////////////////////

//! Performs background archivation of operations.
/*!
 *  \note Thread affinity: control unless noted otherwise
 */
class TOperationsCleaner
    : public TRefCounted
{
public:
    TOperationsCleaner(TOperationsCleanerConfigPtr config, TBootstrap* bootstrap);

    void Start();
    void Stop();

    void SubmitForArchivation(TArchiveOperationRequest request);
    void SubmitForRemoval(TRemoveOperationRequest request);

    void UpdateConfig(const TOperationsCleanerConfigPtr& config);

    void SetArchiveVersion(int version);
    bool IsEnabled() const;

    void BuildOrchid(NYTree::TFluentMap fluent) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TOperationsCleaner)

////////////////////////////////////////////////////////////////////////////////

std::vector<TArchiveOperationRequest> FetchOperationsFromCypressForCleaner(
    const std::vector<TOperationId>& operationIds,
    TCallback<NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr()> createBatchRequest,
    int batchSize);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
