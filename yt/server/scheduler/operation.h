#pragma once

#include "public.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/controller_agent/controller_agent_service.pb.h>

#include <yt/ytlib/scheduler/job_resources.h>
#include <yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/client/api/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/crash_handler.h>
#include <yt/core/misc/dense_map.h>

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/ytree/node.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TOperationEvent
{
    TInstant Time;
    EOperationState State;
};

void Serialize(const TOperationEvent& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TOperationEvent& event, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TControllerAttributes
{
    std::optional<NControllerAgent::TOperationControllerInitializeAttributes> InitializeAttributes;
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
};

void ToProto(
    NControllerAgent::NProto::TControllerTransactionIds* transactionIdsProto,
    const TOperationTransactions& transactions);

void FromProto(
    TOperationTransactions* transactions,
    const NControllerAgent::NProto::TControllerTransactionIds& transactionIdsProto,
    std::function<NApi::NNative::IClientPtr(const NObjectClient::TCellTag&)> getClient,
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

struct IOperationStrategyHost
{
    virtual EOperationType GetType() const = 0;

    virtual bool IsSchedulable() const = 0;

    virtual TInstant GetStartTime() const = 0;

    virtual std::optional<int> FindSlotIndex(const TString& treeId) const = 0;
    virtual int GetSlotIndex(const TString& treeId) const = 0;
    virtual void SetSlotIndex(const TString& treeId, int index) = 0;

    virtual TString GetAuthenticatedUser() const = 0;

    virtual TOperationId GetId() const = 0;

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const = 0;

    virtual NYTree::IMapNodePtr GetSpec() const = 0;

    virtual TOperationRuntimeParametersPtr GetRuntimeParameters() const = 0;
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
    : public TIntrinsicRefCounted
    , public IOperationStrategyHost
{
public:
    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);

    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);

    DEFINE_BYVAL_RO_PROPERTY(EOperationState, State);
    DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(bool, Suspended);

    // By default, all new operations are not activated.
    // When operation passes admission control and scheduler decides
    // that it's ready to start jobs, it is marked as active.
    DEFINE_BYVAL_RW_PROPERTY(bool, Activated);

    DEFINE_BYVAL_RW_PROPERTY(bool, Prepared);

    //! User-supplied transaction where the operation resides.
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::TTransactionId, UserTransactionId);

    DEFINE_BYVAL_RW_PROPERTY(TOperationRuntimeParametersPtr, RuntimeParameters);

    DEFINE_BYVAL_RO_PROPERTY(TOperationRuntimeDataPtr, RuntimeData);

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

    //! Stores statistics about operation preparation and schedule job timings.
    DEFINE_BYREF_RW_PROPERTY(NJobTrackerClient::TStatistics, ControllerTimeStatistics);

    //! Marks that operation attributes should be flushed to Cypress.
    DEFINE_BYVAL_RW_PROPERTY(bool, ShouldFlush);

    //! Brief operation spec.
    DEFINE_BYREF_RW_PROPERTY(NYson::TYsonString, BriefSpec);

    //! If this operation needs revive, the corresponding revive descriptor is provided
    //! by Master Connector.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TOperationRevivalDescriptor>, RevivalDescriptor);

    //! Structure with operation transactions.
    DEFINE_BYREF_RW_PROPERTY(std::optional<TOperationTransactions>, Transactions);

    //! Scheduling tag filters of operation pool trees.
    DEFINE_BYREF_RW_PROPERTY(TPoolTreeToSchedulingTagFilter, PoolTreeToSchedulingTagFilter);

    //! YSON describing suspicous jobs of this operation.
    DEFINE_BYVAL_RW_PROPERTY(NYson::TYsonString, SuspiciousJobs);

    //! Alias for the operation.
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Alias);

    //! Returns operation id.
    TOperationId GetId() const override;

    //! Returns operation start time.
    TInstant GetStartTime() const override;

    //! Returns operation authenticated user.
    TString GetAuthenticatedUser() const override;

    //! Returns operation spec.
    NYTree::IMapNodePtr GetSpec() const override;

    //! Gets set when the operation is started.
    TFuture<TOperationPtr> GetStarted();

    //! Set operation start result.
    void SetStarted(const TError& error);

    //! Gets set when the operation is finished.
    TFuture<void> GetFinished();

    //! Marks the operation as finished.
    void SetFinished();

    //! Delegates to #NYT::NScheduler::IsOperationFinished.
    bool IsFinishedState() const;

    //! Delegates to #NYT::NScheduler::IsOperationFinishing.
    bool IsFinishingState() const;

    //! Checks whether current operation state allows starting new jobs.
    bool IsSchedulable() const override;

    //! Adds new sample to controller time statistics.
    void UpdateControllerTimeStatistics(const NYPath::TYPath& name, TDuration value);

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const override;

    //! Returns the codicil guard holding the operation id.
    TCodicilGuard MakeCodicilGuard() const;

    //! Sets operation state and adds the corresponding event.
    void SetStateAndEnqueueEvent(EOperationState state);

    //! Slot index machinery.
    std::optional<int> FindSlotIndex(const TString& treeId) const override;
    int GetSlotIndex(const TString& treeId) const override;
    void SetSlotIndex(const TString& treeId, int value) override;
    const THashMap<TString, int>& GetSlotIndices() const;

    const std::vector<TString>& GetOwners() const;
    void SetOwners(std::vector<TString> owners);

    NYson::TYsonString BuildAlertsString() const;
    bool HasAlert(EOperationAlertType alertType) const;
    void SetAlert(EOperationAlertType alertType, const TError& error, std::optional<TDuration> timeout = std::nullopt);
    void ResetAlert(EOperationAlertType alertType);

    //! Returns a cancelable control invoker corresponding to this operation.
    const IInvokerPtr& GetCancelableControlInvoker();

    //! Cancels the context of the invoker returned by #GetCancelableControlInvoker.
    void Cancel();

    //! Invokes #Cancel and then recreates the context and the invoker.
    void Restart();

    //! Builds operation result as YSON string.
    NYson::TYsonString BuildResultString() const;

    void SetAgent(const TControllerAgentPtr& agent);
    TControllerAgentPtr GetAgentOrCancelFiber();
    TControllerAgentPtr FindAgent();
    TControllerAgentPtr GetAgentOrThrow();

    TOperation(
        TOperationId operationId,
        EOperationType type,
        const NRpc::TMutationId& mutationId,
        NTransactionClient::TTransactionId userTransactionId,
        NYTree::IMapNodePtr spec,
        NYTree::IMapNodePtr secureVault,
        TOperationRuntimeParametersPtr runtimeParams,
        const TString& authenticatedUser,
        TInstant startTime,
        IInvokerPtr controlInvoker,
        const std::optional<TString>& alias,
        EOperationState state = EOperationState::None,
        const std::vector<TOperationEvent>& events = {},
        bool suspended = false);

private:
    const TOperationId Id_;
    const TInstant StartTime_;
    const TString AuthenticatedUser_;
    const NYTree::IMapNodePtr Spec_;
    const TString CodicilData_;
    const IInvokerPtr ControlInvoker_;


    TCancelableContextPtr CancelableContext_;
    IInvokerPtr CancelableInvoker_;

    THashMap<TString, int> TreeIdToSlotIndex_;

    std::vector<TString> Owners_;

    TOperationAlertMap Alerts_;

    TPromise<void> StartedPromise_ = NewPromise<void>();
    TPromise<void> FinishedPromise_ = NewPromise<void>();

    TWeakPtr<TControllerAgent> Agent_;
};

#undef DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH
#undef DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

class TOperationRuntimeData
    : public TIntrinsicRefCounted
{
public:
    int GetPendingJobCount() const;
    void SetPendingJobCount(int value);

    TJobResources GetNeededResources();
    void SetNeededResources(const TJobResources& value);

    TJobResourcesWithQuotaList GetMinNeededJobResources() const;
    void SetMinNeededJobResources(const TJobResourcesWithQuotaList& value);

private:
    std::atomic<int> PendingJobCount_ = {0};

    mutable NConcurrency::TReaderWriterSpinLock NeededResourcesLock_;
    TJobResources NeededResources_;

    mutable NConcurrency::TReaderWriterSpinLock MinNeededResourcesJobLock_;
    TJobResourcesWithQuotaList MinNeededJobResources_;
};

DEFINE_REFCOUNTED_TYPE(TOperationRuntimeData)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
