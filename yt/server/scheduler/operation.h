#pragma once

#include "public.h"

#include <yt/server/controller_agent/public.h>
#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/crash_handler.h>

#include <yt/core/ytree/node.h>

namespace NYT {
namespace NScheduler {

using namespace NControllerAgent;

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
    TNullable<NControllerAgent::TOperationControllerInitializationAttributes> InitializationAttributes;
    TNullable<NYson::TYsonString> Attributes;
};

////////////////////////////////////////////////////////////////////////////////

//! Per-operation data retrieved from Cypress on handshake.
struct TOperationRevivalDescriptor
{
    NControllerAgent::TControllerTransactionsPtr ControllerTransactions;
    bool UserTransactionAborted = false;
    bool OperationAborting = false;
    bool OperationCommitted = false;
    bool ShouldCommitOutputTransaction = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IOperationStrategyHost
{
    virtual bool IsSchedulable() const = 0;

    virtual TInstant GetStartTime() const = 0;

    virtual TNullable<int> FindSlotIndex(const TString& treeId) const = 0;
    virtual int GetSlotIndex(const TString& treeId) const = 0;
    virtual void SetSlotIndex(const TString& treeId, int index) = 0;

    virtual TString GetAuthenticatedUser() const = 0;

    virtual TOperationId GetId() const = 0;

    virtual IOperationControllerStrategyHostPtr GetControllerStrategyHost() const = 0;

    virtual NYTree::IMapNodePtr GetSpec() const = 0;

    virtual TOperationRuntimeParamsPtr GetRuntimeParams() const = 0;
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

    DEFINE_BYVAL_RW_PROPERTY(bool, Forgotten);

    //! User-supplied transaction where the operation resides.
    DEFINE_BYVAL_RO_PROPERTY(NTransactionClient::TTransactionId, UserTransactionId);

    DEFINE_BYVAL_RW_PROPERTY(TOperationRuntimeParamsPtr, RuntimeParams);

    DEFINE_BYREF_RW_PROPERTY(TControllerAttributes, ControllerAttributes);

    // A YSON map that is stored under ACL in Cypress.
    // NB: It should not be present in operation spec as it may contain
    // sensitive information.
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, SecureVault);

    DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(std::vector<TString>, Owners);

    DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH(TNullable<TInstant>, FinishTime);

    //! List of events that happened to operation.
    DEFINE_BYVAL_RO_PROPERTY(std::vector<TOperationEvent>, Events);

    //! List of operation alerts.
    using TAlerts = TEnumIndexedVector<TError, EOperationAlertType>;
    DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH(TAlerts, Alerts);

    //! Controller that owns the operation.
    DEFINE_BYVAL_RW_PROPERTY(NControllerAgent::IOperationControllerSchedulerHostPtr, Controller);

    //! Operation result, becomes set when the operation finishes.
    DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH(NProto::TOperationResult, Result);

    //! Stores statistics about operation preparation and schedule job timings.
    DEFINE_BYREF_RW_PROPERTY(NJobTrackerClient::TStatistics, ControllerTimeStatistics);

    //! Mark that operation attributes should be flushed to Cypress.
    DEFINE_BYVAL_RW_PROPERTY(bool, ShouldFlush);

    //! Scheduler incarnation that spawned this operation.
    DEFINE_BYVAL_RW_PROPERTY(int, SchedulerIncarnation);

    //! If this operation needs revive, the corresponding revive descriptor is provided
    //! by Master Connector.
    DEFINE_BYREF_RW_PROPERTY(TNullable<TOperationRevivalDescriptor>, RevivalDescriptor);

    //! Cypress storage mode of the operation.
    DEFINE_BYVAL_RO_PROPERTY(EOperationCypressStorageMode, StorageMode);

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

    //! Sets operation state and adds corresponding event.
    void SetState(EOperationState state);

    //! Slot index machinery.
    TNullable<int> FindSlotIndex(const TString& treeId) const override;
    int GetSlotIndex(const TString& treeId) const override;
    void SetSlotIndex(const TString& treeId, int value) override;
    const yhash<TString, int>& GetSlotIndices() const;

    //! Returns a cancelable control invoker corresponding to this operation.
    const IInvokerPtr& GetCancelableControlInvoker();

    //! Cancels the context of the invoker returned by #GetCancelableControlInvoker.
    void Cancel();

    TOperation(
        const TOperationId& operationId,
        EOperationType type,
        const NRpc::TMutationId& mutationId,
        const NTransactionClient::TTransactionId& userTransactionId,
        NYTree::IMapNodePtr spec,
        NYTree::IMapNodePtr secureVault,
        const TString& authenticatedUser,
        const std::vector<TString>& owners,
        TInstant startTime,
        IInvokerPtr controlInvoker,
        EOperationCypressStorageMode storageMode,
        EOperationState state = EOperationState::None,
        bool suspended = false,
        const std::vector<TOperationEvent>& events = {},
        const TNullable<TOperationRevivalDescriptor>& revivalDescriptor = Null);

private:
    const TOperationId Id_;
    const TInstant StartTime_;
    const TString AuthenticatedUser_;
    const NYTree::IMapNodePtr Spec_;

    const TString CodicilData_;
    const TCancelableContextPtr CancelableContext_;
    const IInvokerPtr CancelableInvoker_;

    yhash<TString, int> TreeIdToSlotIndex_;

    TPromise<void> StartedPromise_ = NewPromise<void>();
    TPromise<void> FinishedPromise_ = NewPromise<void>();

};

#undef DEFINE_BYVAL_RW_PROPERTY_FORCE_FLUSH
#undef DEFINE_BYREF_RW_PROPERTY_FORCE_FLUSH

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
