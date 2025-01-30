#pragma once

#include "public.h"

#include <yt/yt/server/lib/scheduler/structs.h>
#include <yt/yt/server/lib/scheduler/transactions.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/codicil.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/yt/compact_containers/compact_flat_map.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TOperationPoolTreeAttributes
    : public NYTree::TYsonStructLite
{
    std::optional<int> SlotIndex;
    bool RunningInEphemeralPool;
    bool RunningInLightweightPool;

    REGISTER_YSON_STRUCT_LITE(TOperationPoolTreeAttributes);

    static void Register(TRegistrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationEvent
{
    TInstant Time;
    EOperationState State;
    NYson::TYsonString Attributes;
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

void Deserialize(TOperationAlert& event, NYTree::INodePtr node);

using TOperationAlertMap = TCompactFlatMap<
    EOperationAlertType,
    TOperationAlert,
    2>;

////////////////////////////////////////////////////////////////////////////////

struct TBriefVanillaTaskSpec
{
    int JobCount;
};

using TBriefVanillaTaskSpecMap = THashMap<std::string, TBriefVanillaTaskSpec>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EUnschedulableReason,
    (IsNotRunning)
    (Suspended)
    (NoPendingAllocations)

    // NB(eshcherbin): This is not exactly an "unschedulable" reason, but it is
    // reasonable in our architecture to put it here anyway.
    (MaxScheduleAllocationCallsViolated)
    (FifoSchedulableElementCountLimitReached)
);

////////////////////////////////////////////////////////////////////////////////

struct IOperationStrategyHost
{
    virtual EOperationType GetType() const = 0;

    virtual EOperationState GetState() const = 0;

    virtual std::optional<EUnschedulableReason> CheckUnschedulable(const std::optional<TString>& treeId = std::nullopt) const = 0;

    virtual TInstant GetStartTime() const = 0;

    virtual std::optional<int> FindSlotIndex(const TString& treeId) const = 0;
    virtual void SetSlotIndex(const TString& treeId, int index) = 0;
    virtual void ReleaseSlotIndex(const TString& treeId) = 0;

    virtual TString GetAuthenticatedUser() const = 0;

    virtual TOperationId GetId() const = 0;

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const = 0;

    virtual TStrategyOperationSpecPtr GetStrategySpec() const = 0;

    virtual TStrategyOperationSpecPtr GetStrategySpecForTree(const TString& treeId) const = 0;

    virtual const NYson::TYsonString& GetSpecString() const = 0;

    virtual const NYson::TYsonString& GetTrimmedAnnotations() const = 0;

    virtual const std::optional<TBriefVanillaTaskSpecMap>& GetMaybeBriefVanillaTaskSpecs() const = 0;

    virtual TOperationRuntimeParametersPtr GetRuntimeParameters() const = 0;

    virtual void UpdatePoolAttributes(
        const TString& treeId,
        const TOperationPoolTreeAttributes& operationPoolTreeAttributes) = 0;

    virtual bool IsTreeErased(const TString& treeId) const = 0;

    virtual void EraseTrees(const std::vector<TString>& treeIds) = 0;

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
    static_assert(true)

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
    } \
    static_assert(true)

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TRefCounted
    , public IOperationStrategyHost
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);

    DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(bool, Suspended);

    DEFINE_BYVAL_RW_PROPERTY(TError, OrhanedOperationAbortionError);

public:
    // By default, all new operations are not activated.
    // When operation passes admission control and scheduler decides
    // that it's ready to start allocations, it is marked as active.
    bool IsRunningInStrategy() const;
    void SetRunningInStrategy();

    //! User-supplied transaction where the operation resides.
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::TTransactionId, UserTransactionId);

    DEFINE_BYREF_RW_PROPERTY(TControllerAttributes, ControllerAttributes);

    DEFINE_BYVAL_RW_PROPERTY(bool, RevivedFromSnapshot);
    DEFINE_BYREF_RW_PROPERTY(std::vector<NScheduler::TAllocationPtr>, RevivedAllocations);

    // A YSON map that is stored under ACL in Cypress.
    // NB: It should not be present in operation spec as it may contain
    // sensitive information.
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, SecureVault);

    //! Stores the id of the Cypress node corresponding to the temporary
    //! token issued for the operation, if any.
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NCypressClient::TNodeId>, TemporaryTokenNodeId);

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

    //! Operation spec provided by user.
    DEFINE_BYREF_RO_PROPERTY(NYson::TYsonString, ProvidedSpecString);

    //! If this operation needs revive, the corresponding revive descriptor is provided
    //! by Master Connector.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TOperationRevivalDescriptor>, RevivalDescriptor);

    //! Structure with operation transactions.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TOperationTransactions>, Transactions);

    //! Pool tree information for operation controller.
    DEFINE_BYREF_RW_PROPERTY(TPoolTreeControllerSettingsMap, PoolTreeControllerSettingsMap);

    //! YSON describing suspicious jobs of this operation.
    DEFINE_BYVAL_RW_PROPERTY(NYson::TYsonString, SuspiciousJobs);

    //! Alias for the operation.
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Alias);

    //! ACEs that are always included in operation ACL
    //! regardless any ACL specification and any ACL changes made by user.
    DEFINE_BYREF_RO_PROPERTY(NSecurityClient::TSerializableAccessControlList, BaseAcl);

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

    //! Returns operation annotations used for structured logging.
    const NYson::TYsonString& GetTrimmedAnnotations() const override;

    //! Returns names of operation tasks.
    //! Works for vanilla operations only.
    std::vector<std::string> GetTaskNames() const;

    const std::optional<TBriefVanillaTaskSpecMap>& GetMaybeBriefVanillaTaskSpecs() const override;

    //! Gets set when the operation is started.
    TFuture<TOperationPtr> GetStarted();

    //! Returns ACL or ACO name for operation.
    TAccessControlRule GetAccessControlRule() const;

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

    //! Checks whether current operation state doesn't allow starting new allocations.
    std::optional<EUnschedulableReason> CheckUnschedulable(const std::optional<TString>& treeId) const override;

    IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override;

    //! Returns the codicil guard holding the operation id.
    TCodicilGuard MakeCodicilGuard() const;

    EOperationState GetState() const override;

    //! Sets operation state and adds the corresponding event with given attributes.
    void SetStateAndEnqueueEvent(
        EOperationState state,
        NYson::TYsonString attributes = {});

    //! Slot index machinery.
    std::optional<int> FindSlotIndex(const TString& treeId) const override;
    void SetSlotIndex(const TString& treeId, int value) override;
    void ReleaseSlotIndex(const TString& treeId) override;
    THashMap<TString, int> GetSlotIndices() const;
    const THashMap<TString, TOperationPoolTreeAttributes>& GetSchedulingAttributesPerPoolTree() const;

    TOperationRuntimeParametersPtr GetRuntimeParameters() const override;
    void SetRuntimeParameters(TOperationRuntimeParametersPtr parameters);
    void UpdatePoolAttributes(
        const TString& treeId,
        const TOperationPoolTreeAttributes& operationPoolTreeAttributes) override;

    NYson::TYsonString BuildAlertsString() const;
    bool HasAlert(EOperationAlertType alertType) const;
    bool HasAlertResetCookie(EOperationAlertType alertType) const;

    // NB: These methods does not save alert events to archive.
    // Probably you should use TScheduler::SetOperationAlert().
    bool SetAlertWithoutArchivation(EOperationAlertType alertType, const TError& error);
    void ResetAlertWithoutArchivation(EOperationAlertType alertType);
    void SetAlertResetCookie(EOperationAlertType alertType, NConcurrency::TDelayedExecutorCookie cookie);

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
    TControllerAgentPtr FindAgent();
    TControllerAgentPtr GetAgentOrThrow();

    bool IsTreeErased(const TString& treeId) const override;
    bool AreAllTreesErased() const;
    void EraseTrees(const std::vector<TString>& treeIds) override;

    //! Returns vector of experiment assignment names with each
    //! name being of form "<experiment name>.<group name>".
    std::vector<TString> GetExperimentAssignmentNames() const;

    std::vector<TString> GetJobShellOwners(const TString& jobShellName);

    // Aborts all transactions except user and "completion" transactions.
    TFuture<void> AbortCommonTransactions();

    //! Adds token to secure vault according to the operation spec.
    //! Requires that the operation is in `Starting` state, token issuance is request in spec,
    //! and that the secure vault does not contain the key specified in the operation spec.
    void SetTemporaryToken(const TString& token, const NCypressClient::TNodeId& nodeId);

    //! Returns a list of Cypress nodes which must be deleted alongside the operation node.
    std::vector<NCypressClient::TNodeId> GetDependentNodeIds() const;

    TOperation(
        TOperationId operationId,
        EOperationType type,
        NRpc::TMutationId mutationId,
        NTransactionClient::TTransactionId userTransactionId,
        TOperationSpecBasePtr spec,
        THashMap<TString, TStrategyOperationSpecPtr> customSpecPerTree,
        NYson::TYsonString specString,
        NYson::TYsonString trimmedAnnotations,
        std::optional<TBriefVanillaTaskSpecMap> briefVanillaTaskSpecs,
        NYTree::IMapNodePtr secureVault,
        std::optional<NCypressClient::TNodeId> temporaryTokenNodeId,
        TOperationRuntimeParametersPtr runtimeParameters,
        NSecurityClient::TSerializableAccessControlList baseAcl,
        const std::string& authenticatedUser,
        TInstant startTime,
        IInvokerPtr controlInvoker,
        const std::optional<TString>& alias,
        std::vector<TExperimentAssignmentPtr> experimentAssignments,
        NYson::TYsonString providedSpecString,
        EOperationState state = EOperationState::None,
        const std::vector<TOperationEvent>& events = {},
        bool suspended = false,
        int registrationIndex = 0,
        const THashMap<EOperationAlertType, TOperationAlert>& alerts = {});

private:
    const TOperationId Id_;
    const EOperationType Type_;
    const TInstant StartTime_;
    const TString AuthenticatedUser_;
    const NYson::TYsonString SpecString_;
    const NYson::TYsonString TrimmedAnnotations_;
    const std::optional<TBriefVanillaTaskSpecMap> BriefVanillaTaskSpecs_;
    const THashMap<TString, TStrategyOperationSpecPtr> CustomSpecPerTree_;
    const std::string Codicil_;
    const IInvokerPtr ControlInvoker_;

    bool RunningInStrategy_ = false;

    EOperationState State_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    THashMap<TString, TOperationPoolTreeAttributes> SchedulingAttributesPerPoolTree_;

    TOperationRuntimeParametersPtr RuntimeParameters_;

    TOperationAlertMap Alerts_;

    TPromise<void> StartedPromise_ = NewPromise<void>();
    TPromise<void> FinishedPromise_ = NewPromise<void>();

    bool Unregistering_ = false;

    TWeakPtr<TControllerAgent> Agent_;

    //! Aggregated minimum needed resources at the start of the operation.
    std::optional<TJobResources> AggregatedInitialMinNeededResources_;

    //! Adds key-value pair to secure vault. Returns true if the entry was added.
    //! May only be called while operation is in `Starting` state.
    //! NB: Be careful with this method with respect to secure vault persistence in Cypress.
    bool AddSecureVaultEntry(const TString& key, const NYTree::INodePtr& value);
};

#undef DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH
#undef DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

struct TPreprocessedSpec
{
    TOperationSpecBasePtr Spec;
    NYson::TYsonString SpecString;
    NYson::TYsonString ProvidedSpecString;
    NYson::TYsonString TrimmedAnnotations;
    THashMap<TString, TStrategyOperationSpecPtr> CustomSpecPerTree;
    std::vector<TExperimentAssignmentPtr> ExperimentAssignments;
    std::vector<TError> ExperimentAssignmentErrors;
    std::optional<THashMap<std::string, TBriefVanillaTaskSpec>> BriefVanillaTaskSpecs;
};

//! Fill various spec parts of preprocessed spec.
void ParseSpec(
    NYTree::IMapNodePtr specNode,
    NYTree::INodePtr specTemplate,
    EOperationType operationType,
    std::optional<TOperationId> operationId,
    TPreprocessedSpec* preprocessedSpec);

//! A helper that wraps YSON parsing error or invalid node type error into
//! convenient "Error parsing operation spec string" error.
NYTree::IMapNodePtr ConvertSpecStringToNode(
    const NYson::TYsonString& specString,
    int treeSizeLimit = std::numeric_limits<int>::max());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
