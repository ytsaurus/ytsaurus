#pragma once

#include "public.h"
#include "operation.h"

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

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
    // Archive version >= 17
    NYson::TYsonString FullSpec;
    NYson::TYsonString UnrecognizedSpec;
    // Archive version >= 22
    NYson::TYsonString RuntimeParameters;
    // Archive version >= 26
    std::optional<TString> Alias;
    // Archive version >= 27
    NYson::TYsonString SlotIndexPerPoolTree;

    void InitializeFromOperation(const TOperationPtr& operation);

    static const std::vector<TString>& GetAttributeKeys();
    static const std::vector<TString>& GetProgressAttributeKeys();
    void InitializeFromAttributes(const NYTree::IAttributeDictionary& attributes);
};

////////////////////////////////////////////////////////////////////////////////

struct TRemoveOperationRequest
{
    TOperationId Id;
};

////////////////////////////////////////////////////////////////////////////////

struct IOperationsCleanerHost
{
    virtual ~IOperationsCleanerHost() = default;
    virtual void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) = 0;
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
    TOperationsCleaner(
        TOperationsCleanerConfigPtr config,
        IOperationsCleanerHost* host,
        TBootstrap* bootstrap);

    void Start();
    void Stop();

    void SubmitForArchivation(TArchiveOperationRequest request);
    void SubmitForRemoval(TRemoveOperationRequest request);

    void UpdateConfig(const TOperationsCleanerConfigPtr& config);

    void SetArchiveVersion(int version);
    bool IsEnabled() const;

    void BuildOrchid(NYTree::TFluentMap fluent) const;

    //! Raised when a new portion of operations has been archived.
    DECLARE_SIGNAL(void(const std::vector<TArchiveOperationRequest>&), OperationsArchived);

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

} // namespace NYT::NScheduler
