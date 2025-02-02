#include "ground_update_queue_manager.h"

#include "private.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>
#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/master/sequoia_server/proto/ground_update_queue_manager.pb.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/ytlib/sequoia_client/transaction.h>
#include <yt/yt/ytlib/sequoia_client/client.h>

#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NProto;
using namespace NSequoiaClient;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SequoiaServerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TTableUpdateQueueRecord
{
    NSequoiaClient::ESequoiaTable Table;
    NTableClient::TUnversionedOwningRow Row;
    i64 SequenceNumber;
    EGroundUpdateAction Action;

    void Persist(const NCellMaster::TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, Table);
        Persist(context, Row);
        Persist(context, SequenceNumber);
        Persist(context, Action);
    }
};

struct TTableUpdateQueueState
{
    std::deque<TTableUpdateQueueRecord> Records;
    TTransactionId OngoingFlushTransactionId;
    i64 NextRecordSequenceNumber = 0;
    i64 LastFlushedSequenceNumber = 0;

    bool IsFlushNeeded() const
    {
        return !Records.empty() && !OngoingFlushTransactionId;
    }

    void Persist(const NCellMaster::TPersistenceContext& context)
    {
        using NYT::Persist;

        Persist(context, Records);
        Persist(context, OngoingFlushTransactionId);
        Persist(context, NextRecordSequenceNumber);
        Persist(context, LastFlushedSequenceNumber);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGroundUpdateQueueManager
    : public IGroundUpdateQueueManager
    , public TMasterAutomatonPart
{
public:
    explicit TGroundUpdateQueueManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::GroundUpdateQueueManager)
    {
        RegisterLoader(
            "GroundUpdateQueueManager",
            BIND_NO_PROPAGATE(&TGroundUpdateQueueManager::Load, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "GroundUpdateQueueManager",
            BIND_NO_PROPAGATE(&TGroundUpdateQueueManager::Save, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        YT_VERIFY(transactionManager);
        transactionManager->RegisterTransactionActionHandlers<TReqFlushGroundUpdateQueue>({
            .Prepare = BIND_NO_PROPAGATE(&TGroundUpdateQueueManager::HydraPrepareFlushTableUpdateQueue, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TGroundUpdateQueueManager::HydraCommitFlushTableUpdateQueue, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TGroundUpdateQueueManager::HydraAbortFlushTableUpdateQueue, Unretained(this)),
        });

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TGroundUpdateQueueManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void EnqueueRow(
        EGroundUpdateQueue queue,
        ESequoiaTable table,
        TUnversionedOwningRow row,
        EGroundUpdateAction action) override
    {
        YT_VERIFY(HasMutationContext());

        const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->SequoiaManager;
        if (!config->EnableGroundUpdateQueues) {
            return;
        }

        auto& queueState = QueueStates_[queue];

        TTableUpdateQueueRecord record{
            .Table = table,
            .Row = std::move(row),
            .SequenceNumber = queueState.NextRecordSequenceNumber++,
            .Action = action,
        };

        YT_LOG_DEBUG("Table update queue record enqueued (Queue: %v, SequenceNumber: %v, Table: %v, Action: %v, Row: %v)",
            queue,
            record.SequenceNumber,
            record.Table,
            record.Action,
            record.Row);

        queueState.Records.push_back(std::move(record));
    }

private:
    TEnumIndexedArray<EGroundUpdateQueue, TPeriodicExecutorPtr> FlushExecutors_;
    TEnumIndexedArray<EGroundUpdateQueue, TTableUpdateQueueState> QueueStates_;


    void HydraPrepareFlushTableUpdateQueue(
        TTransaction* transaction,
        TReqFlushGroundUpdateQueue* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& /*options*/)
    {
        auto queue = FromProto<EGroundUpdateQueue>(request->queue());
        auto& queueState = QueueStates_[queue];

        if (queueState.OngoingFlushTransactionId) {
            THROW_ERROR_EXCEPTION("Queue is already being flushed by transaction %v", queueState.OngoingFlushTransactionId);
        }

        if (queueState.Records.empty()) {
            auto error = TError("There are no updates to flush");
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }
        if (queueState.Records.front().SequenceNumber != request->start_sequence_number()) {
            auto error = TError("First record sequence number %v is different from requested sequence number %v",
                queueState.Records.front().SequenceNumber,
                request->start_sequence_number());
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }
        if (queueState.Records.back().SequenceNumber < request->end_sequence_number()) {
            auto error = TError("Last queue sequence number %v is less than requested sequence number %v",
                queueState.Records.back().SequenceNumber,
                request->end_sequence_number());
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }

        queueState.OngoingFlushTransactionId = transaction->GetId();

        YT_LOG_DEBUG("Table update queue flush prepared (Queue: %v, TransactionId: %v)",
            queue,
            queueState.OngoingFlushTransactionId);
    }

    void HydraCommitFlushTableUpdateQueue(
        TTransaction* transaction,
        TReqFlushGroundUpdateQueue* request,
        const NTransactionSupervisor::TTransactionCommitOptions& /*options*/)
    {
        auto queue = FromProto<EGroundUpdateQueue>(request->queue());
        auto& queueState = QueueStates_[queue];

        YT_VERIFY(queueState.OngoingFlushTransactionId == transaction->GetId());
        queueState.OngoingFlushTransactionId = {};

        auto startSequenceNumber = request->start_sequence_number();
        auto endSequenceNumber = request->end_sequence_number();
        YT_VERIFY(startSequenceNumber <= endSequenceNumber);

        YT_VERIFY(!queueState.Records.empty() && queueState.Records.front().SequenceNumber == startSequenceNumber);

        i64 lastSequenceNumber = -1;
        int recordCount = 0;

        while (!queueState.Records.empty() && queueState.Records.front().SequenceNumber <= endSequenceNumber) {
            lastSequenceNumber = queueState.Records.front().SequenceNumber;
            ++recordCount;
            queueState.Records.pop_front();
        }

        // Ensure we actually have all requested records in queue.
        YT_VERIFY(lastSequenceNumber == endSequenceNumber);
        YT_VERIFY(queueState.LastFlushedSequenceNumber <= endSequenceNumber);
        queueState.LastFlushedSequenceNumber = endSequenceNumber;

        YT_LOG_DEBUG("Table update queue records flushed (Queue: %v, TransactionId: %v, StartSequenceNumber: %v, EndSequenceNumber: %v, RecordCount: %v)",
            queue,
            transaction->GetId(),
            startSequenceNumber,
            endSequenceNumber,
            recordCount);
    }

    void HydraAbortFlushTableUpdateQueue(
        TTransaction* transaction,
        TReqFlushGroundUpdateQueue* request,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
    {
        auto queue = FromProto<EGroundUpdateQueue>(request->queue());
        auto& queueState = QueueStates_[queue];

        if (queueState.OngoingFlushTransactionId == transaction->GetId()) {
            queueState.OngoingFlushTransactionId = {};
            YT_LOG_DEBUG("Table update queue flush aborted (Queue: %v, TransactionId: %v)",
                queue,
                transaction->GetId());
        } else {
            YT_LOG_DEBUG("Table update queue flush abort ignored (Queue: %v, TransactionId: %v, OngoingFlushTransactionId: %v)",
                queue,
                transaction->GetId(),
                queueState.OngoingFlushTransactionId);
        }
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        const auto& config = GetDynamicConfig();
        for (auto queue : TEnumTraits<EGroundUpdateQueue>::GetDomainValues()) {
            const auto& queueConfig = config->GetQueueConfig(queue);
            auto executor = New<TPeriodicExecutor>(
                Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::GroundUpdateQueueManager),
                BIND(&TGroundUpdateQueueManager::OnQueueFlush, MakeWeak(this), queue),
                queueConfig->FlushPeriod);
            executor->Start();
            FlushExecutors_[queue] = std::move(executor);
        }
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        for (auto queue : TEnumTraits<EGroundUpdateQueue>::GetDomainValues()) {
            if (auto& executor = FlushExecutors_[queue]) {
                YT_UNUSED_FUTURE(executor->Stop());
            }
        }
        FlushExecutors_ = {};
    }

    void OnQueueFlush(EGroundUpdateQueue queue)
    {
        const auto& config = GetDynamicConfig();
        const auto& queueConfig = config->GetQueueConfig(queue);

        if (queueConfig->PauseFlush) {
            return;
        }

        auto& queueState = QueueStates_[queue];

        if (!queueState.IsFlushNeeded()) {
            return;
        }

        Y_UNUSED(WaitFor(Bootstrap_
            ->GetSequoiaClient()
            ->StartTransaction()
            .Apply(BIND([queue, this, this_ = MakeStrong(this)] (const ISequoiaTransactionPtr& transaction) {
                auto& queueState = QueueStates_[queue];

                if (!queueState.IsFlushNeeded()) {
                    return VoidFuture;
                }

                const auto& config = GetDynamicConfig();
                const auto& queueConfig = config->GetQueueConfig(queue);

                auto startSequenceNumber = queueState.Records.front().SequenceNumber;
                auto recordsToFlush = std::min<int>(std::size(queueState.Records), queueConfig->FlushBatchSize);
                // If there are several records from one mutation this might not actually be
                // the last record to flush.
                const auto& lastRecordToFlush = queueState.Records[recordsToFlush - 1];

                auto endSequenceNumber = lastRecordToFlush.SequenceNumber;

                TReqFlushGroundUpdateQueue request;
                request.set_queue(ToProto(queue));
                request.set_start_sequence_number(startSequenceNumber);
                request.set_end_sequence_number(endSequenceNumber);

                YT_LOG_DEBUG("Started flushing table update queue records (Queue: %v, StartSequenceNumber: %v, EndSequenceNumber: %v)",
                    queue,
                    startSequenceNumber,
                    endSequenceNumber);

                for (const auto& record : queueState.Records) {
                    if (record.SequenceNumber > endSequenceNumber) {
                        break;
                    }

                    // TODO(aleksandra-zh): remove this logging when sequoia queues are stable.
                    YT_LOG_DEBUG("Flushing table update queue row (Queue: %v, SequenceNumber: %v, Action: %v, Row: %v)",
                        queue,
                        record.SequenceNumber,
                        record.Action,
                        record.Row.Get());

                    switch (record.Action) {
                        case EGroundUpdateAction::Write:
                            transaction->WriteRow(
                                record.Table,
                                record.Row.Get());
                            break;

                        case EGroundUpdateAction::Delete:
                            transaction->DeleteRow(
                                record.Table,
                                record.Row.Get());
                            break;

                        default:
                            YT_ABORT();
                    }
                }

                transaction->AddTransactionAction(
                    Bootstrap_->GetCellTag(),
                    NTransactionClient::MakeTransactionActionData(request));

                NApi::TTransactionCommitOptions commitOptions{
                    .CoordinatorCellId = Bootstrap_->GetCellId(),
                    .CoordinatorPrepareMode = NApi::ETransactionCoordinatorPrepareMode::Late,
                    .StronglyOrdered = true,
                };
                return transaction->Commit(commitOptions);
            }).AsyncVia(EpochAutomatonInvoker_))));
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        const auto& config = GetDynamicConfig();
        for (auto queue : TEnumTraits<EGroundUpdateQueue>::GetDomainValues()) {
            const auto& queueConfig = config->GetQueueConfig(queue);
            if (const auto& executor = FlushExecutors_[queue]) {
                executor->SetPeriod(queueConfig->FlushPeriod);
            }
        }
    }

    const TDynamicGroundUpdateQueueManagerConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->GroundUpdateQueueManager;
    }

    void Clear() override
    {
        QueueStates_ = {};
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, QueueStates_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        Load(context, QueueStates_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IGroundUpdateQueueManagerPtr CreateGroundUpdateQueueManager(TBootstrap* bootstrap)
{
    return New<TGroundUpdateQueueManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
