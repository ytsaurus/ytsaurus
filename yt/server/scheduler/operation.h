#pragma once

#include "public.h"

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/scheduler/scheduler_service.pb.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/ref.h>

#include <yt/core/ytree/node.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TOperation
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOperationId, Id);

    DEFINE_BYVAL_RO_PROPERTY(EOperationType, Type);

    DEFINE_BYVAL_RO_PROPERTY(NRpc::TMutationId, MutationId);

    DEFINE_BYVAL_RW_PROPERTY(EOperationState, State);
    DEFINE_BYVAL_RW_PROPERTY(bool, Suspended);

    // By default, all new operations are not activated.
    // When operation passes admission control and scheduler decides
    // that it's ready to start jobs, it is marked as active.
    DEFINE_BYVAL_RW_PROPERTY(bool, Activated);

    DEFINE_BYVAL_RW_PROPERTY(bool, Prepared);

    //! User-supplied transaction where the operation resides.
    DEFINE_BYVAL_RO_PROPERTY(NApi::ITransactionPtr, UserTransaction);

    //! Transaction used for maintaining operation inputs and outputs.
    /*!
     *  SyncSchedulerTransaction is nested inside UserTransaction, if any.
     *  Input and output transactions are nested inside SyncSchedulerTransaction.
     */
    DEFINE_BYVAL_RW_PROPERTY(NApi::ITransactionPtr, SyncSchedulerTransaction);

    //! Transaction used for internal housekeeping, e.g. generating stderrs.
    /*!
     *  Not nested inside any other transaction.
     */
    DEFINE_BYVAL_RW_PROPERTY(NApi::ITransactionPtr, AsyncSchedulerTransaction);

    //! Transaction used for taking snapshot of operation input.
    /*!
     *  InputTransaction is nested inside SyncSchedulerTransaction.
     */
    DEFINE_BYVAL_RW_PROPERTY(NApi::ITransactionPtr, InputTransaction);

    //! Transaction used for locking and writing operation output.
    /*!
     *  OutputTransaction is nested inside SyncSchedulerTransaction.
     */
    DEFINE_BYVAL_RW_PROPERTY(NApi::ITransactionPtr, OutputTransaction);

    //! |true| if transactions are initialized and should be refreshed.
    DEFINE_BYVAL_RW_PROPERTY(bool, HasActiveTransactions);

    DEFINE_BYVAL_RO_PROPERTY(NYTree::IMapNodePtr, Spec);

    DEFINE_BYVAL_RO_PROPERTY(Stroka, AuthenticatedUser);
    DEFINE_BYVAL_RO_PROPERTY(std::vector<Stroka>, Owners);

    DEFINE_BYVAL_RO_PROPERTY(TInstant, StartTime);
    DEFINE_BYVAL_RW_PROPERTY(TNullable<TInstant>, FinishTime);

    //! Number of stderrs generated so far.
    DEFINE_BYVAL_RW_PROPERTY(int, StderrCount);

    //! Maximum number of stderrs to capture.
    DEFINE_BYVAL_RW_PROPERTY(int, MaxStderrCount);

    //! Scheduling tag.
    DEFINE_BYVAL_RW_PROPERTY(TNullable<Stroka>, SchedulingTag);

    //! Currently existing jobs in the operation.
    DEFINE_BYREF_RW_PROPERTY(yhash_set<TJobPtr>, Jobs);

    //! Controller that owns the operation.
    DEFINE_BYVAL_RW_PROPERTY(IOperationControllerPtr, Controller);

    //! Operation result, becomes set when the operation finishes.
    DEFINE_BYREF_RW_PROPERTY(NProto::TOperationResult, Result);

    DEFINE_BYREF_RW_PROPERTY(NJobTrackerClient::TStatistics, ControllerTimeStatistics);

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
    bool IsSchedulable() const;

    void UpdateControllerTimeStatistics(const NYPath::TYPath& name, TDuration value);

    //! Returns |true| if operation controller progress can be built.
    bool HasControllerProgress() const;

    TOperation(
        const TOperationId& operationId,
        EOperationType type,
        const NRpc::TMutationId& mutationId,
        NApi::ITransactionPtr userTransaction,
        NYTree::IMapNodePtr spec,
        const Stroka& authenticatedUser,
        const std::vector<Stroka>& owners,
        TInstant startTime,
        EOperationState state = EOperationState::Initializing,
        bool suspended = false);

private:
    TPromise<void> StartedPromise_ = NewPromise<void>();
    TPromise<void> FinishedPromise_ = NewPromise<void>();

};

DEFINE_REFCOUNTED_TYPE(TOperation)

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
