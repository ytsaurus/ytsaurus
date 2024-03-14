#include "sequoia_queue_manager.h"

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

#include <yt/yt/server/master/sequoia_server/proto/sequoia_queue_manager.pb.h>

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

static const auto& Logger = SequoiaServerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TSequoiaQueueRecord
{
    NSequoiaClient::ESequoiaTable Table;
    NTableClient::TUnversionedOwningRow Row;
    i64 SequenceNumber;
    ESequoiaRecordAction Action;

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, Table);
        Save(context, Row);
        Save(context, SequenceNumber);
        Save(context, Action);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        Load(context, Table);
        Load(context, Row);
        Load(context, SequenceNumber);
        Load(context, Action);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSequoiaQueueManager
    : public ISequoiaQueueManager
    , public TMasterAutomatonPart
{
public:
    explicit TSequoiaQueueManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::SequoiaQueueManager)
    {
        RegisterLoader(
            "SequoiaQueueManager",
            BIND(&TSequoiaQueueManager::Load, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "SequoiaQueueManager",
            BIND(&TSequoiaQueueManager::Save, Unretained(this)));
    }

    void EnqueueRow(
        ESequoiaTable table,
        TUnversionedOwningRow row,
        ESequoiaRecordAction action) override
    {
        YT_VERIFY(HasMutationContext());

        TSequoiaQueueRecord record{
            .Table = table,
            .Row = std::move(row),
            .SequenceNumber = NextRecordSequenceNumber_++,
            .Action = action
        };

        YT_LOG_DEBUG("Record enqueued (SequenceNumber: %v, Table: %v, Action: %v, Row: %v)",
            record.SequenceNumber,
            record.Table,
            record.Action,
            record.Row);

        Records_.push_back(std::move(record));
    }

private:
    TPeriodicExecutorPtr QueueFlushExecutor_;

    std::deque<TSequoiaQueueRecord> Records_;
    TTransactionId OngoingFlushTransactionId_;
    i64 NextRecordSequenceNumber_ = 0;
    i64 LastFlushedSequenceNumber_ = 0;

    void Initialize() override
    {
        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        YT_VERIFY(transactionManager);
        transactionManager->RegisterTransactionActionHandlers<TReqFlushQueue>({
            .Prepare = BIND_NO_PROPAGATE(&TSequoiaQueueManager::HydraPrepareFlushQueue, Unretained(this)),
            .Commit = BIND_NO_PROPAGATE(&TSequoiaQueueManager::HydraCommitFlushQueue, Unretained(this)),
            .Abort = BIND_NO_PROPAGATE(&TSequoiaQueueManager::HydraAbortFlushQueue, Unretained(this)),
        });

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TSequoiaQueueManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void HydraPrepareFlushQueue(
        TTransaction* transaction,
        TReqFlushQueue* request,
        const NTransactionSupervisor::TTransactionPrepareOptions& /*options*/)
    {
        if (OngoingFlushTransactionId_) {
            THROW_ERROR_EXCEPTION("Queue is already being flushed by transaction %v", OngoingFlushTransactionId_);
        }

        if (Records_.empty()) {
            auto error = TError("There are no records to flush");
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }
        if (Records_.front().SequenceNumber != request->start_sequence_number()) {
            auto error = TError("First record sequence number %v is different from requested sequence number %v",
                Records_.front().SequenceNumber,
                request->start_sequence_number());
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }
        if (Records_.back().SequenceNumber < request->end_sequence_number()) {
            auto error = TError("Last queue sequence number %v is less than requested sequence number %v",
                Records_.back().SequenceNumber,
                request->end_sequence_number());
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }

        OngoingFlushTransactionId_ = transaction->GetId();
    }

    void HydraCommitFlushQueue(
        TTransaction* transaction,
        TReqFlushQueue* request,
        const NTransactionSupervisor::TTransactionCommitOptions& /*options*/)
    {
        YT_VERIFY(OngoingFlushTransactionId_ == transaction->GetId());
        OngoingFlushTransactionId_ = {};

        auto startSequenceNumber = request->start_sequence_number();
        auto endSequenceNumber = request->end_sequence_number();
        YT_VERIFY(startSequenceNumber <= endSequenceNumber);

        YT_VERIFY(!Records_.empty() && Records_.front().SequenceNumber == startSequenceNumber);

        i64 lastSequenceNumber = -1;
        int recordCount = 0;

        while (!Records_.empty() && Records_.front().SequenceNumber <= endSequenceNumber) {
            lastSequenceNumber = Records_.front().SequenceNumber;
            ++recordCount;
            Records_.pop_front();
        }

        // Ensure we actually have all requested records in queue.
        YT_VERIFY(lastSequenceNumber == endSequenceNumber);
        YT_VERIFY(LastFlushedSequenceNumber_ <= endSequenceNumber);
        LastFlushedSequenceNumber_ = endSequenceNumber;

        YT_LOG_DEBUG("Records flushed (StartSequenceNumber: %v, EndSequenceNumber: %v, RecordCount: %v)",
            startSequenceNumber,
            endSequenceNumber,
            recordCount);
    }

    void HydraAbortFlushQueue(
        TTransaction* transaction,
        TReqFlushQueue* /*request*/,
        const NTransactionSupervisor::TTransactionAbortOptions& /*options*/)
    {
        if (OngoingFlushTransactionId_ == transaction->GetId()) {
            OngoingFlushTransactionId_ = {};
        }
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        QueueFlushExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(EAutomatonThreadQueue::SequoiaQueueManager),
            BIND(&TSequoiaQueueManager::OnQueueFlush, MakeWeak(this)),
            GetDynamicConfig()->FlushPeriod);
        QueueFlushExecutor_->Start();
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        if (QueueFlushExecutor_) {
            YT_UNUSED_FUTURE(QueueFlushExecutor_->Stop());
            QueueFlushExecutor_.Reset();
        }
    }

    bool IsQueueFlushNeeded()
    {
        return !Records_.empty() && !OngoingFlushTransactionId_;
    }

    void OnQueueFlush()
    {
        if (GetDynamicConfig()->PauseFlush) {
            return;
        }

        if (!IsQueueFlushNeeded()) {
            return;
        }

        Y_UNUSED(WaitFor(Bootstrap_
            ->GetSequoiaClient()
            ->StartTransaction()
            .Apply(BIND([this, this_ = MakeStrong(this)] (const ISequoiaTransactionPtr& transaction) {
                if (!IsQueueFlushNeeded()) {
                    return VoidFuture;
                }

                auto startSequenceNumber = Records_.front().SequenceNumber;
                auto recordsToFlush = std::min<int>(std::size(Records_), GetDynamicConfig()->FlushBatchSize);
                // If there are several records from one mutation this might not actually be
                // the last record to flush.
                const auto& lastRecordToFlush = Records_[recordsToFlush - 1];

                auto endSequenceNumber = lastRecordToFlush.SequenceNumber;

                TReqFlushQueue request;
                request.set_start_sequence_number(startSequenceNumber);
                request.set_end_sequence_number(endSequenceNumber);

                YT_LOG_DEBUG("Started flushing records (StartSequenceNumber: %v, EndSequenceNumber: %v)",
                    startSequenceNumber,
                    endSequenceNumber);

                for (const auto& record : Records_) {
                    if (record.SequenceNumber > endSequenceNumber) {
                        break;
                    }

                    // TODO(aleksandra-zh): remove this logging when sequoia queues are stable.
                    YT_LOG_DEBUG("Flushing row (SequenceNumber: %v, Row: %v)",
                        record.SequenceNumber,
                        record.Row.Get());

                    switch (record.Action) {
                        case ESequoiaRecordAction::Write:
                            transaction->WriteRow(
                                record.Table,
                                record.Row.Get());
                            break;

                        case ESequoiaRecordAction::Delete:
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
                };

                return transaction->Commit(commitOptions);
            }).AsyncVia(EpochAutomatonInvoker_))));
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        if (QueueFlushExecutor_) {
            QueueFlushExecutor_->SetPeriod(GetDynamicConfig()->FlushPeriod);
        }
    }

    const TDynamicSequoiaQueueConfigPtr& GetDynamicConfig() const
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->SequoiaManager->SequoiaQueue;
    }

    void Clear() override
    {
        Records_.clear();
        OngoingFlushTransactionId_ = {};
        LastFlushedSequenceNumber_ = 0;
        NextRecordSequenceNumber_ = 0;
    }

    void Save(NCellMaster::TSaveContext& context) const
    {
        using NYT::Save;

        Save(context, Records_);
        Save(context, OngoingFlushTransactionId_);
        Save(context, LastFlushedSequenceNumber_);
        Save(context, NextRecordSequenceNumber_);
    }

    void Load(NCellMaster::TLoadContext& context)
    {
        using NYT::Load;

        Load(context, Records_);
        Load(context, OngoingFlushTransactionId_);
        Load(context, LastFlushedSequenceNumber_);
        Load(context, NextRecordSequenceNumber_);
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaQueueManagerPtr CreateSequoiaQueueManager(TBootstrap* bootstrap)
{
    return New<TSequoiaQueueManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
