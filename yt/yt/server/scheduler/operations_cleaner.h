#pragma once

#include "public.h"
#include "operation.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TRemoveOperationRequest
{
    TOperationId Id;
    //! It is guaranteed that all dependent nodes will be successfully
    //! removed before the operation node is removed itself.
    std::vector<NCypressClient::TNodeId> DependentNodeIds;
};

struct TArchiveOperationRequest
    : public TRemoveOperationRequest
{
    // Id is present in TRemoveOperationRequest.
    TInstant StartTime;
    TInstant FinishTime;
    EOperationState State;
    std::string AuthenticatedUser;
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
    NYson::TYsonString SchedulingAttributesPerPoolTree;
    // COMPAT(omgronny)
    NYson::TYsonString SlotIndexPerPoolTree;
    NYson::TYsonString TaskNames;
    // Archive version >= 40
    NYson::TYsonString ExperimentAssignments;
    NYson::TYsonString ExperimentAssignmentNames;
    // Archive version >= 42
    NYson::TYsonString ControllerFeatures;
    // Archive version >= 46
    NYson::TYsonString ProvidedSpec;

    static const std::vector<TString>& GetAttributeKeys();
    static const std::vector<TString>& GetProgressAttributeKeys();
};

////////////////////////////////////////////////////////////////////////////////

struct IOperationsCleanerHost
{
    virtual ~IOperationsCleanerHost() = default;
    virtual void SetSchedulerAlert(ESchedulerAlertType alertType, const TError& alert) = 0;
    virtual IInvokerPtr GetBackgroundInvoker() const = 0;
    virtual IInvokerPtr GetOperationsCleanerInvoker() const = 0;
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

    void SubmitForRemoval(std::vector<TRemoveOperationRequest> requests);

    void UpdateConfig(const TOperationsCleanerConfigPtr& config);

    void SetArchiveVersion(int version);
    bool IsEnabled() const;

    void BuildOrchid(NYTree::TFluentMap fluent) const;

    //! NB: Alert events recording can be incorrect in case of scheduler crashes.
    //! E.g. alert turns off, we persist it's absence in operation's Cypress node.
    //! Then scheduler crashes without sending alert event to archive.
    //! If alert doesn't appear later, we will end up with infinite alert in alerts history.
    void EnqueueOperationAlertEvent(
        TOperationId operationId,
        EOperationAlertType alertType,
        TError alert);

    //! Raised when a new portion of operations has been archived.
    DECLARE_SIGNAL(void(const std::vector<TArchiveOperationRequest>&), OperationsRemovedFromCypress);

    TArchiveOperationRequest InitializeRequestFromOperation(const TOperationPtr& operation);
    TArchiveOperationRequest InitializeRequestFromAttributes(const NYTree::IAttributeDictionary& attributes);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TOperationsCleaner)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
