#pragma once

#include "public.h"
#include "operation.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/fluent.h>

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
    NYson::TYsonString FullSpec;
    NYson::TYsonString UnrecognizedSpec;
    NYson::TYsonString RuntimeParameters;
    std::optional<TString> Alias;
    NYson::TYsonString SlotIndexPerPoolTree;
    NYson::TYsonString TaskNames;
    // Archive version >= 40
    NYson::TYsonString ExperimentAssignments;
    NYson::TYsonString ExperimentAssignmentNames;
    // Archive version >= 42
    NYson::TYsonString ControllerFeatures;
    // Archive version >= 46
    NYson::TYsonString ProvidedSpec;

    void InitializeFromOperation(const TOperationPtr& operation);

    static const std::vector<TString>& GetAttributeKeys();
    static const std::vector<TString>& GetProgressAttributeKeys();
    void InitializeFromAttributes(const NYTree::IAttributeDictionary& attributes);
};

////////////////////////////////////////////////////////////////////////////////

struct IOperationsCleanerHost
{
    virtual ~IOperationsCleanerHost() = default;
    virtual void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) = 0;
    virtual IInvokerPtr GetBackgroundInvoker() const = 0;
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

    ~TOperationsCleaner();

    void Start();
    void Stop();

    void SubmitForArchivation(TArchiveOperationRequest request);
    void SubmitForArchivation(std::vector<TOperationId> operations);

    void SubmitForRemoval(std::vector<TOperationId> operations);

    void UpdateConfig(const TOperationsCleanerConfigPtr& config);

    void SetArchiveVersion(int version);
    bool IsEnabled() const;

    void BuildOrchid(NYTree::TFluentMap fluent) const;

    //! NB: Alert events recording can be incorrect in case of scheduler crashes.
    //! E.g. alert turns off, we persist it's absense in operation's Cypress node.
    //! Then scheduler crashes without sending alert event to archive.
    //! If alert doesn't appear later, we will end up with infinite alert in alerts history.
    void EnqueueOperationAlertEvent(
        TOperationId operationId,
        EOperationAlertType alertType,
        const TError& alert);

    //! Raised when a new portion of operations has been archived.
    DECLARE_SIGNAL(void(const std::vector<TArchiveOperationRequest>&), OperationsRemovedFromCypress);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TOperationsCleaner)

////////////////////////////////////////////////////////////////////////////////

std::vector<TArchiveOperationRequest> FetchOperationsFromCypressForCleaner(
    const std::vector<TOperationId>& operationIds,
    TCallback<NObjectClient::TObjectServiceProxy::TReqExecuteBatchPtr()> createBatchRequest,
    const int parseOperationAttributesBatchSize,
    const IInvokerPtr& invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
