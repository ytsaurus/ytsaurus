#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/job_resources.h>
#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/ref.h>
#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/dense_map.h>

#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/node.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TOperationEvent
{
    TInstant Time;
    EOperationState State;
    THashMap<TString, TString> Attributes;
};

void Serialize(const TOperationEvent& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TOperationEvent& event, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TControllerAttributes
{
    std::optional<TOperationControllerInitializeAttributes> InitializeAttributes;
    NYson::TYsonString PrepareAttributes;
};

////////////////////////////////////////////////////////////////////////////////

// NB: Keep sync with NControllerAgent::TControllerTransactionIds.
struct TOperationTransactions
{
    NApi::ITransactionPtr AsyncTransaction;
    NApi::ITransactionPtr InputTransaction;
    NApi::ITransactionPtr OutputTransaction;
    NApi::ITransactionPtr DebugTransaction;
    NApi::ITransactionPtr OutputCompletionTransaction;
    NApi::ITransactionPtr DebugCompletionTransaction;
    std::vector<NApi::ITransactionPtr> NestedInputTransactions;
};

void ToProto(
    NControllerAgent::NProto::TControllerTransactionIds* transactionIdsProto,
    const TOperationTransactions& transactions);

void FromProto(
    TOperationTransactions* transactions,
    const NControllerAgent::NProto::TControllerTransactionIds& transactionIdsProto,
    std::function<NApi::NNative::IClientPtr(NObjectClient::TCellTag)> getClient,
    TDuration pingPeriod);

////////////////////////////////////////////////////////////////////////////////

//! Per-operation data retrieved from Cypress on handshake.
struct TOperationRevivalDescriptor
{
    bool UserTransactionAborted = false;
    bool OperationAborting = false;
    bool OperationCommitted = false;
    bool ShouldCommitOutputTransaction = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationAlert
{
    TError Error;
    NConcurrency::TDelayedExecutorCookie ResetCookie;
};

using TOperationAlertMap = SmallDenseMap<
    EOperationAlertType,
    TOperationAlert,
    2,
    TEnumTraits<EOperationAlertType>::TDenseMapInfo>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnschedulableReason,
    (IsNotRunning)
    (Suspended)
    (NoPendingJobs)

    // NB(eshcherbin): This is not exactly an "unschedulable" reason, but it is
    // reasonable in our architecture to put it here anyway.
    (MaxScheduleJobCallsViolated)
);

////////////////////////////////////////////////////////////////////////////////

struct IOperationStrategyHost
{
    virtual EOperationType GetType() const = 0;

    virtual EOperationState GetState() const = 0;

    virtual std::optional<EUnschedulableReason> CheckUnschedulable() const = 0;

    virtual TInstant GetStartTime() const = 0;

    virtual std::optional<int> FindSlotIndex(const TString& treeId) const = 0;
    virtual int GetSlotIndex(const TString& treeId) const = 0;
    virtual void SetSlotIndex(const TString& treeId, int index) = 0;

    virtual TString GetAuthenticatedUser() const = 0;

    virtual TOperationId GetId() const = 0;

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const = 0;

    virtual TStrategyOperationSpecPtr GetStrategySpec() const = 0;

    virtual TStrategyOperationSpecPtr GetStrategySpecForTree(const TString& treeId) const = 0;

    virtual const NYson::TYsonString& GetSpecString() const = 0;

    virtual TOperationRuntimeParametersPtr GetRuntimeParameters() const = 0;

    virtual bool IsTreeErased(const TString& treeId) const = 0;

    virtual void EraseTrees(const std::vector<TString>& treeIds) = 0;

    virtual std::optional<TJobResources> GetInitialAggregatedMinNeededResources() const = 0;

protected:
    friend class TFairShareStrategyOperationState;
};

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(type, name, ...) \
protected: \
    type name##_ { __VA_ARGS__ }; \
    \
public: \
    Y_FORCE_INLINE type Get##name() const \
    { \
        return name##_; \
    } \
    \
    Y_FORCE_INLINE void Set##name(type value) \
    { \
        name##_ = value; \
        ShouldFlush_ = true; \
    } \

#define DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH(type, name, ...) \
protected: \
    type name##_ { __VA_ARGS__ }; \
    \
public: \
    Y_FORCE_INLINE type& Mutable##name() \
    { \
        ShouldFlush_ = true; \
        return name##_; \
    } \
    \
    Y_FORCE_INLINE const type& name() const \
    { \
        return name##_; \
    }

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TRefCounted
    , public IOperationStrategyHost
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);

    DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(bool, Suspended);

public:
    // By default, all new operations are not activated.
    // When operation passes admission control and scheduler decides
    // that it's ready to start jobs, it is marked as active.
    bool IsRunningInStrategy() const;
    void SetRunningInStrategy();

    //! User-supplied transaction where the operation resides.
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::TTransactionId, UserTransactionId);

    DEFINE_BYREF_RW_PROPERTY(TControllerAttributes, ControllerAttributes);

    DEFINE_BYVAL_RW_PROPERTY(bool, RevivedFromSnapshot);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NScheduler::TJobPtr>, RevivedJobs);

    // A YSON map that is stored under ACL in Cypress.
    // NB: It should not be present in operation spec as it may contain
    // sensitive information.
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, SecureVault);

    //! Marks that operation ACL should be flushed to Cypress.
    DEFINE_BYVAL_RW_PROPERTY(bool, ShouldFlushAcl);

    DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(std::optional<TInstant>, FinishTime);

    //! List of events that happened to operation.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TOperationEvent>, Events);

    DEFINE_BYVAL_RW_PROPERTY(IOperationControllerPtr, Controller);

    //! Operation result, becomes set when the operation finishes.
    DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH(NProto::TOperationResult, Result);

    //! Marks that operation attributes should be flushed to Cypress.
    DEFINE_BYVAL_RW_PROPERTY(bool, ShouldFlush);

    //! Brief operation spec.
    DEFINE_BYREF_RW_PROPERTY(NYson::TYsonString, BriefSpecString);

    //! Operation spec.
    DEFINE_BYREF_RO_PROPERTY(TOperationSpecBasePtr, Spec);

    //! If this operation needs revive, the corresponding revive descriptor is provided
    //! by Master Connector.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TOperationRevivalDescriptor>, RevivalDescriptor);

    //! Structure with operation transactions.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TOperationTransactions>, Transactions);

    //! Pool tree information for operation controller.
    DEFINE_BYREF_RW_PROPERTY(TPoolTreeControllerSettingsMap, PoolTreeControllerSettingsMap);

    //! YSON describing suspicous jobs of this operation.
    DEFINE_BYVAL_RW_PROPERTY(NYson::TYsonString, SuspiciousJobs);

    //! Alias for the operation.
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Alias);

    //! ACEs that are always included in operation ACL
    //! regardless any ACL specification and any ACL changes made by user.
    DEFINE_BYREF_RO_PROPERTY(NSecurityClient::TSerializableAccessControlList, BaseAcl);

    //! Aggregated minimum needed resources at the start of the operation.
    DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(std::optional<TJobResources>, InitialAggregatedMinNeededResources);

    //! List of assigned experiments.
    DEFINE_BYREF_RO_PROPERTY(std::vector<TExperimentAssignmentPtr>, ExperimentAssignments);

    //! Index of operation to fix order of registration at master connector.
    DEFINE_BYREF_RO_PROPERTY(int, RegistrationIndex, 0);

    //! Index that is incremented after each operation revival.
    DEFINE_BYREF_RW_PROPERTY(TControllerEpoch, ControllerEpoch, 0);

public:
    //! Returns operation id.
    TOperationId GetId() const override;

    EOperationType GetType() const override;

    //! Returns operation start time.
    TInstant GetStartTime() const override;

    //! Returns operation authenticated user.
    TString GetAuthenticatedUser() const override;

    //! Returns strategy operation spec.
    TStrategyOperationSpecPtr GetStrategySpec() const override;

    //! Returns strategy operation spec patched for given tere.
    TStrategyOperationSpecPtr GetStrategySpecForTree(const TString& treeId) const override;

    //! Returns operation spec as a yson string.
    const NYson::TYsonString& GetSpecString() const override;

    //! Returns names of operation tasks.
    //! Works for vanilla operations only.
    std::vector<TString> GetTaskNames() const;

    //! Gets set when the operation is started.
    TFuture<TOperationPtr> GetStarted();

    //! Set operation start result.
    void SetStarted(const TError& error);

    //! Gets set when the operation is finished.
    TFuture<void> GetFinished();

    //! Marks the operation as finished.
    void SetFinished();
    
    //! Gets set when the operation is finished and start unregistering.
    bool GetUnregistering() const;

    //! Marks operation as unregistering.
    void SetUnregistering();

    //! Delegates to #NYT::NScheduler::IsOperationFinished.
    bool IsFinishedState() const;

    //! Delegates to #NYT::NScheduler::IsOperationFinishing.
    bool IsFinishingState() const;

    //! Checks whether current operation state doesn't allow starting new jobs.
    std::optional<EUnschedulableReason> CheckUnschedulable() const override;

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override;

    //! Returns the codicil guard holding the operation id.
    TCodicilGuard MakeCodicilGuard() const;

    virtual EOperationState GetState() const override;

    //! Sets operation state and adds the corresponding event with given attributes.
    void SetStateAndEnqueueEvent(
        EOperationState state,
        const THashMap<TString, TString>& attributes = {});

    //! Slot index machinery.
    std::optional<int> FindSlotIndex(const TString& treeId) const override;
    int GetSlotIndex(const TString& treeId) const override;
    void SetSlotIndex(const TString& treeId, int value) override;
    const THashMap<TString, int>& GetSlotIndices() const;

    TOperationRuntimeParametersPtr GetRuntimeParameters() const override;
    void SetRuntimeParameters(TOperationRuntimeParametersPtr parameters);

    NYson::TYsonString BuildAlertsString() const;
    bool HasAlert(EOperationAlertType alertType) const;
    void SetAlert(EOperationAlertType alertType, const TError& error, std::optional<TDuration> timeout = std::nullopt);
    void ResetAlert(EOperationAlertType alertType);

    //! Returns a control invoker corresponding to this operation.
    const IInvokerPtr& GetControlInvoker();

    //! Returns a cancelable control invoker corresponding to this operation.
    const IInvokerPtr& GetCancelableControlInvoker();

    //! Cancels the context of the invoker returned by #GetCancelableControlInvoker.
    void Cancel(const TError& error);

    //! Invokes #Cancel and then recreates the context and the invoker.
    void Restart(const TError& error);

    //! Builds operation result as YSON string.
    NYson::TYsonString BuildResultString() const;

    void SetAgent(const TControllerAgentPtr& agent);
    TControllerAgentPtr GetAgentOrCancelFiber();
    TControllerAgentPtr FindAgent();
    TControllerAgentPtr GetAgentOrThrow();

    virtual bool IsTreeErased(const TString& treeId) const override;

    virtual void EraseTrees(const std::vector<TString>& treeIds) override;

    //! Returns vector of experiment assignment names with each
    //! name being of form "<experiment name>.<group name>".
    std::vector<TString> GetExperimentAssignmentNames() const;

    TOperation(
        TOperationId operationId,
        EOperationType type,
        NRpc::TMutationId mutationId,
        NTransactionClient::TTransactionId userTransactionId,
        TOperationSpecBasePtr spec,
        THashMap<TString, TStrategyOperationSpecPtr> customSpecPerTree,
        NYson::TYsonString specString,
        NYTree::IMapNodePtr secureVault,
        TOperationRuntimeParametersPtr runtimeParameters,
        NSecurityClient::TSerializableAccessControlList baseAcl,
        const TString& authenticatedUser,
        TInstant startTime,
        IInvokerPtr controlInvoker,
        const std::optional<TString>& alias,
        std::vector<TExperimentAssignmentPtr> experimentAssignments,
        EOperationState state = EOperationState::None,
        const std::vector<TOperationEvent>& events = {},
        bool suspended = false,
        const std::optional<TJobResources>& initialAggregatedMinNeededResources = {},
        int registrationIndex = 0);

private:
    const TOperationId Id_;
    const EOperationType Type_;
    const TInstant StartTime_;
    const TString AuthenticatedUser_;
    const NYson::TYsonString SpecString_;
    const THashMap<TString, TStrategyOperationSpecPtr> CustomSpecPerTree_;
    const TString CodicilData_;
    const IInvokerPtr ControlInvoker_;

    bool RunningInStrategy_ = false;

    EOperationState State_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    THashMap<TString, int> TreeIdToSlotIndex_;

    TOperationRuntimeParametersPtr RuntimeParameters_;

    TOperationAlertMap Alerts_;

    TPromise<void> StartedPromise_ = NewPromise<void>();
    TPromise<void> FinishedPromise_ = NewPromise<void>();

    bool Unregistering_ = false;

    TWeakPtr<TControllerAgent> Agent_;
};

#undef DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH
#undef DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

struct TPreprocessedSpec
{
    TOperationSpecBasePtr Spec;
    NYTree::IMapNodePtr SpecNode;
    NYson::TYsonString SpecString;
    THashMap<TString, TStrategyOperationSpecPtr> CustomSpecPerTree;
    std::vector<TExperimentAssignmentPtr> ExperimentAssignments;
};

//! Fill various spec parts of preprocessed spec.
void ParseSpec(
    NYTree::IMapNodePtr specNode,
    NYTree::INodePtr specTemplate,
    std::optional<TOperationId> operationId,
    TPreprocessedSpec* preprocessedSpec);

//! A helper that wraps YSON parsing error or invalid node type error into
//! convenient "Error parsing operation spec string" error.
NYTree::IMapNodePtr ConvertSpecStringToNode(const NYson::TYsonString& specString);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
