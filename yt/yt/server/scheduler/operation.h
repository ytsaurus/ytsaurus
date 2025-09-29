#pragma once

#include "public.h"

#include <yt/yt/server/scheduler/strategy/operation.h>
#include <yt/yt/server/scheduler/strategy/operation_controller.h>

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

struct TPatchSpecInProgressInfo
{
    std::string User;
    TInstant StartTime;
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
    : public NStrategy::IOperation
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

    //! Use it like a lock to prevent concurrent update_op_parameters.
    //! If set, contains info about update in progress.
    //! nullopt means that there is no update in progress
    DEFINE_BYREF_RW_PROPERTY(std::optional<TPatchSpecInProgressInfo>, PatchSpecInProgress);

    //! Nullable.
    DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH(NYTree::INodePtr, CumulativeSpecPatch);
    DEFINE_BYVAL_RW_PROPERTY(bool, ShouldFlushSpecPatch, false);

public:
    //! Returns operation id.
    TOperationId GetId() const override;

    EOperationType GetType() const override;

    //! Returns operation start time.
    TInstant GetStartTime() const override;

    //! Returns operation authenticated user.
    std::string GetAuthenticatedUser() const override;

    //! Returns operation title.
    std::optional<std::string> GetTitle() const override;

    //! Returns strategy operation spec.
    TStrategyOperationSpecPtr GetStrategySpec() const override;

    //! Returns strategy operation spec patched for given tere.
    TStrategyOperationSpecPtr GetStrategySpecForTree(const std::string& treeId) const override;

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
    std::optional<NStrategy::EUnschedulableReason> CheckUnschedulable(const std::optional<std::string>& treeId) const override;

    NStrategy::ISchedulingOperationControllerPtr GetControllerStrategyHost() const override;

    //! Returns the codicil guard holding the operation id.
    TCodicilGuard MakeCodicilGuard() const;

    EOperationState GetState() const override;

    const TOperationOptionsPtr& GetOperationOptions() const override;

    //! Sets operation state and adds the corresponding event with given attributes.
    void SetStateAndEnqueueEvent(
        EOperationState state,
        NYson::TYsonString attributes = {});

    //! Slot index machinery.
    std::optional<int> FindSlotIndex(const std::string& treeId) const override;
    void SetSlotIndex(const std::string& treeId, int value) override;
    void ReleaseSlotIndex(const std::string& treeId) override;
    THashMap<std::string, int> GetSlotIndices() const;
    const THashMap<std::string, NStrategy::TOperationPoolTreeAttributes>& GetSchedulingAttributesPerPoolTree() const;

    TOperationRuntimeParametersPtr GetRuntimeParameters() const override;
    void SetRuntimeParameters(TOperationRuntimeParametersPtr parameters);
    void UpdatePoolAttributes(
        const std::string& treeId,
        const NStrategy::TOperationPoolTreeAttributes& operationPoolTreeAttributes) override;

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

    bool IsTreeErased(const std::string& treeId) const override;
    bool AreAllTreesErased() const;
    void EraseTrees(const std::vector<std::string>& treeIds) override;

    //! Returns vector of experiment assignment names with each
    //! name being of form "<experiment name>.<group name>".
    std::vector<TString> GetExperimentAssignmentNames() const;

    std::vector<std::string> GetJobShellOwners(const TString& jobShellName);

    // Aborts all transactions except user and "completion" transactions.
    TFuture<void> AbortCommonTransactions();

    //! Adds token to secure vault according to the operation spec.
    //! Requires that the operation is in `Starting` state, token issuance is request in spec,
    //! and that the secure vault does not contain the key specified in the operation spec.
    void SetTemporaryToken(const std::string& token, const NCypressClient::TNodeId& nodeId);

    //! Returns a list of Cypress nodes which must be deleted alongside the operation node.
    std::vector<NCypressClient::TNodeId> GetDependentNodeIds() const;

    TOperation(
        TOperationId operationId,
        EOperationType type,
        NRpc::TMutationId mutationId,
        NTransactionClient::TTransactionId userTransactionId,
        TOperationSpecBasePtr spec,
        THashMap<std::string, TStrategyOperationSpecPtr> customSpecPerTree,
        NYson::TYsonString specString,
        NYson::TYsonString trimmedAnnotations,
        std::optional<TBriefVanillaTaskSpecMap> briefVanillaTaskSpecs,
        NYTree::IMapNodePtr secureVault,
        std::optional<NCypressClient::TNodeId> temporaryTokenNodeId,
        TOperationRuntimeParametersPtr runtimeParameters,
        TOperationOptionsPtr operationOptions,
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
        const THashMap<EOperationAlertType, TOperationAlert>& alerts = {},
        NYTree::INodePtr cumulativeSpecPatch = nullptr);

private:
    const TOperationId Id_;
    const EOperationType Type_;
    const TInstant StartTime_;
    const std::string AuthenticatedUser_;
    const NYson::TYsonString SpecString_;
    const NYson::TYsonString TrimmedAnnotations_;
    const std::optional<TBriefVanillaTaskSpecMap> BriefVanillaTaskSpecs_;
    const THashMap<std::string, TStrategyOperationSpecPtr> CustomSpecPerTree_;
    const TOperationOptionsPtr OperationOptions_;
    const std::string Codicil_;
    const IInvokerPtr ControlInvoker_;

    bool RunningInStrategy_ = false;

    EOperationState State_;

    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    THashMap<std::string, NStrategy::TOperationPoolTreeAttributes> SchedulingAttributesPerPoolTree_;

    TOperationRuntimeParametersPtr RuntimeParameters_;

    TOperationAlertMap Alerts_;

    const TPromise<void> StartedPromise_ = NewPromise<void>();
    const TPromise<void> FinishedPromise_ = NewPromise<void>();

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
    THashMap<std::string, TStrategyOperationSpecPtr> CustomSpecPerTree;
    std::vector<TExperimentAssignmentPtr> ExperimentAssignments;
    std::vector<TError> ExperimentAssignmentErrors;
    std::optional<THashMap<std::string, TBriefVanillaTaskSpec>> BriefVanillaTaskSpecs;
    TOperationOptionsPtr OperationOptions;
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

TBriefVanillaTaskSpecMap GetBriefVanillaTaskSpecs(const NYTree::IMapNodePtr& specNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
