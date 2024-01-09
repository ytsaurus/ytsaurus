#pragma once

#include <yt/yt/core/test_framework/framework.h>

#include "sorted_store_manager_ut_helpers.h"
#include "ordered_dynamic_store_ut_helpers.h"
#include "simple_transaction_supervisor.h"
#include "simple_tablet_manager.h"

#include <yt/yt/server/node/tablet_node/automaton.h>
#include <yt/yt/server/node/tablet_node/bootstrap.h>
#include <yt/yt/server/node/tablet_node/mutation_forwarder.h>
#include <yt/yt/server/node/tablet_node/serialize.h>
#include <yt/yt/server/node/tablet_node/tablet.h>
#include <yt/yt/server/node/tablet_node/tablet_slot.h>
#include <yt/yt/server/node/tablet_node/transaction.h>
#include <yt/yt/server/node/tablet_node/transaction_manager.h>

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_lease_tracker.h>

#include <yt/yt/server/lib/hydra/mock/simple_hydra_manager_mock.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NTabletNode {
namespace {

// TODO(max42): split into .cpp and .h.

using namespace NConcurrency;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NSecurityClient;
using namespace NTransactionClient;
using namespace NTransactionSupervisor;
using namespace NHydra;
using namespace NHiveServer;
using namespace NRpc;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

class TSimpleTabletSlot
    : public ITransactionManagerHost
{
public:
    static constexpr TCellId CellId = {0, 42};
    static constexpr auto CellTag = TCellTag(42);

    explicit TSimpleTabletSlot(TTabletOptions options)
    {
        AutomatonQueue_ = New<TActionQueue>("Automaton");
        AutomatonInvoker_ = AutomatonQueue_->GetInvoker();

        Automaton_ = New<TTabletAutomaton>(
            CellId,
            /*asyncSnapshotInvoker*/ AutomatonInvoker_,
            /*leaseManager*/ nullptr);
        Automaton_->RegisterWaitTimeObserver([&] (TDuration mutationWaitTime) {
            TotalMutationWaitTime_ += mutationWaitTime.MicroSeconds() + 1;
        });

        HydraManager_ = New<TSimpleHydraManagerMock>(Automaton_, AutomatonInvoker_, NTabletNode::GetCurrentReign());
        TransactionManager_ = CreateTransactionManager(New<TTransactionManagerConfig>(), /*transactionManagerHost*/ this, InvalidCellTag, CreateNullTransactionLeaseTracker());
        TransactionSupervisor_ = New<TSimpleTransactionSupervisor>(TransactionManager_, HydraManager_, Automaton_, AutomatonInvoker_);
        TabletManager_ = New<TSimpleTabletManager>(TransactionManager_, HydraManager_, Automaton_, AutomatonInvoker_);
        TabletCellWriteManager_ = CreateTabletCellWriteManager(
            TabletManager_,
            HydraManager_,
            Automaton_,
            AutomatonInvoker_,
            CreateDummyMutationForwarder());

        TabletManager_->InitializeTablet(options);
        TabletCellWriteManager_->Initialize();
    }

    NHydra::ISimpleHydraManagerPtr GetSimpleHydraManager() override
    {
        return HydraManager_;
    }

    const NHydra::TCompositeAutomatonPtr& GetAutomaton() override
    {
        return Automaton_;
    }

    IInvokerPtr GetAutomatonInvoker(EAutomatonThreadQueue /*queue*/ = EAutomatonThreadQueue::Default) override
    {
        return AutomatonInvoker_;
    }

    IInvokerPtr GetEpochAutomatonInvoker(EAutomatonThreadQueue /*queue*/ = EAutomatonThreadQueue::Default) override
    {
        return AutomatonInvoker_;
    }

    IInvokerPtr GetGuardedAutomatonInvoker(EAutomatonThreadQueue /*queue*/ = EAutomatonThreadQueue::Default) override
    {
        return AutomatonInvoker_;
    }

    const IMutationForwarderPtr& GetMutationForwarder() override
    {
        static IMutationForwarderPtr dummyMutationForwarder = CreateDummyMutationForwarder();
        return dummyMutationForwarder;
    }

    ITabletManagerPtr GetTabletManager() override
    {
        return nullptr;
    }

    void Shutdown()
    {
        YT_VERIFY(HydraManager_->GetPendingMutationCount() == 0);
        AutomatonQueue_->Shutdown(/*graceful*/ true);
        AutomatonQueue_.Reset();
        AutomatonInvoker_.Reset();
        Automaton_.Reset();
        HydraManager_.Reset();
        TransactionManager_.Reset();
        TransactionSupervisor_.Reset();
        TabletManager_.Reset();
        TabletCellWriteManager_.Reset();
    }

    const ITransactionSupervisorPtr& GetTransactionSupervisor() override
    {
        // Lease checking is disabled, so transaction supervisor is not needed.
        YT_UNIMPLEMENTED();
    }

    const TRuntimeTabletCellDataPtr& GetRuntimeData() override
    {
        static TRuntimeTabletCellDataPtr RuntimeTabletCellData = nullptr;
        return RuntimeTabletCellData;
    }

    NTransactionClient::TTimestamp GetLatestTimestamp() override
    {
        return LatestTimestamp_;
    }

    NObjectClient::TCellTag GetNativeCellTag() override
    {
        return TCellTag();
    }

    const NApi::NNative::IConnectionPtr& GetNativeConnection() override
    {
        return DummyNativeConnection_;
    }

    NHydra::TCellId GetCellId() override
    {
        return CellId;
    }

    void SetLatestTimestamp(TTimestamp timestamp)
    {
        LatestTimestamp_ = timestamp;
    }

    const TSimpleTabletManagerPtr& TabletManager()
    {
        return TabletManager_;
    }

    const ITabletCellWriteManagerPtr& TabletCellWriteManager()
    {
        return TabletCellWriteManager_;
    }

    const TSimpleHydraManagerMockPtr& HydraManager()
    {
        return HydraManager_;
    }

    const ITransactionManagerPtr& TransactionManager()
    {
        return TransactionManager_;
    }

    const TSimpleTransactionSupervisorPtr& TransactionSupervisor()
    {
        return TransactionSupervisor_;
    }

    TDuration GetTotalMutationWaitTime()
    {
        return TDuration::MicroSeconds(TotalMutationWaitTime_.load());
    }

private:
    TSimpleHydraManagerMockPtr HydraManager_;
    TActionQueuePtr AutomatonQueue_;
    IInvokerPtr AutomatonInvoker_;
    TCompositeAutomatonPtr Automaton_;
    ITransactionManagerPtr TransactionManager_;
    TSimpleTransactionSupervisorPtr TransactionSupervisor_;
    TSimpleTabletManagerPtr TabletManager_;
    ITabletCellWriteManagerPtr TabletCellWriteManager_;
    NApi::NNative::IConnectionPtr DummyNativeConnection_;

    std::atomic<i64> TotalMutationWaitTime_;

    TTimestamp LatestTimestamp_ = 4242;
};

DECLARE_REFCOUNTED_CLASS(TSimpleTabletSlot)
DEFINE_REFCOUNTED_TYPE(TSimpleTabletSlot)

////////////////////////////////////////////////////////////////////////////////

class TTabletCellWriteManagerTestBase
    : public testing::Test
{
protected:
    TSimpleTabletSlotPtr TabletSlot_;

    virtual TTabletOptions GetOptions() const = 0;

    void SetUp() override
    {
        TabletSlot_ = New<TSimpleTabletSlot>(GetOptions());
    }

    void TearDown() override
    {
        TabletSlot_->Shutdown();
    }

    const ITabletCellWriteManagerPtr& TabletCellWriteManager()
    {
        return TabletSlot_->TabletCellWriteManager();
    }

    IInvokerPtr AutomatonInvoker()
    {
        return TabletSlot_->GetAutomatonInvoker();
    }

    TSimpleHydraManagerMockPtr HydraManager()
    {
        return TabletSlot_->HydraManager();
    }

    ITransactionManagerPtr TransactionManager()
    {
        return TabletSlot_->TransactionManager();
    }

    TSimpleTransactionSupervisorPtr TransactionSupervisor()
    {
        return TabletSlot_->TransactionSupervisor();
    }

    TTabletId MakeTabletTransactionId(TTimestamp timestamp, int hash = 0, EAtomicity atomicity = EAtomicity::Full)
    {
        return NTransactionClient::MakeTabletTransactionId(atomicity, TSimpleTabletSlot::CellTag, timestamp, hash);
    }

    auto RunInAutomaton(auto callable)
    {
        auto result = WaitFor(
            BIND(callable)
                .AsyncVia(AutomatonInvoker())
                .Run());
        if constexpr (!std::is_same_v<std::decay_t<decltype(result)>, TError>) {
            return result
                .ValueOrThrow();
        } else {
            result
                .ThrowOnError();
            return;
        }
    }

    // Recall that this method may wait on blocked row.

    TFuture<void> WriteUnversionedRows(
        TTransactionId transactionId,
        std::vector<TUnversionedOwningRow> rows,
        TTransactionSignature prepareSignature,
        TTransactionSignature commitSignature,
        TTransactionGeneration generation)
    {
        auto* tablet = TabletSlot_->TabletManager()->GetTablet();
        auto tabletSnapshot = tablet->BuildSnapshot(nullptr);
        return BIND([
            transactionId,
            rows = std::move(rows),
            prepareSignature,
            commitSignature,
            generation,
            tabletWriteManager = TabletCellWriteManager(),
            tabletSnapshot
        ] {
            auto writer = CreateWireProtocolWriter();
            i64 dataWeight = 0;
            for (const auto& row : rows) {
                writer->WriteCommand(EWireProtocolCommand::WriteRow);
                writer->WriteUnversionedRow(row);
                dataWeight += GetDataWeight(row);
            }
            auto wireData = writer->Finish();
            struct TTag {};
            auto reader = CreateWireProtocolReader(MergeRefsToRef<TTag>(wireData));
            auto future = tabletWriteManager->Write(
                tabletSnapshot,
                reader.get(),
                TTabletCellWriteParams{
                    .TransactionId = transactionId,
                    .TransactionStartTimestamp = TimestampFromTransactionId(transactionId),
                    .TransactionTimeout = TDuration::Max(),
                    .PrepareSignature = prepareSignature,
                    .CommitSignature = commitSignature,
                    .Generation = generation,
                    .RowCount = static_cast<int>(std::ssize(rows)),
                    .DataWeight = dataWeight
                });

            // NB: we are not going to return future since it will be set only when
            // WriteRows mutation (or mutations) are applied; we are applying mutations
            // manually in these unittests, so this future is meaningless.
            // Still, it is useful to check that no error is thrown in WriteRows mutation handler.
            future
                .Subscribe(BIND([] (const TError& error) {
                    YT_VERIFY(error.IsOK());
                }));
        })
            .AsyncVia(AutomatonInvoker())
            .Run();
    }

    TFuture<void> WriteUnversionedRows(
        TTransactionId transactionId,
        std::vector<TUnversionedOwningRow> rows,
        TTransactionSignature signature = -1,
        TTransactionGeneration generation = 0)
    {
        return WriteUnversionedRows(
            transactionId,
            rows,
            /*prepareSignature*/ signature,
            /*commitSignature*/ signature,
            generation);
    }

    void WriteDelayedUnversionedRows(
        TTransactionId transactionId,
        std::vector<TUnversionedOwningRow> rows,
        TTransactionSignature commitSignature)
    {
        auto* tablet = TabletSlot_->TabletManager()->GetTablet();
        RunInAutomaton([&] {
            auto writer = CreateWireProtocolWriter();
            i64 dataWeight = 0;
            for (const auto& row : rows) {
                writer->WriteCommand(EWireProtocolCommand::WriteRow);
                writer->WriteUnversionedRow(row);
                dataWeight += GetDataWeight(row);
            }

            NProto::TReqWriteDelayedRows request;
            ToProto(request.mutable_transaction_id(), transactionId);
            ToProto(request.mutable_tablet_id(), tablet->GetId());
            struct TTag {};
            request.set_compressed_data(ToString(MergeRefsToRef<TTag>(writer->Finish())));
            request.set_commit_signature(commitSignature);
            request.set_lockless(true);
            request.set_row_count(rows.size());
            request.set_data_weight(dataWeight);

            auto mutation = CreateMutation(HydraManager(), request);
            YT_UNUSED_FUTURE(mutation->Commit());
        });
    }

    TFuture<void> WriteVersionedRows(
        TTransactionId transactionId,
        std::vector<TVersionedOwningRow> rows,
        TTransactionSignature signature = -1)
    {
        auto* tablet = TabletSlot_->TabletManager()->GetTablet();
        auto tabletSnapshot = tablet->BuildSnapshot(nullptr);
        return BIND([transactionId, rows = std::move(rows), signature, tabletWriteManager = TabletCellWriteManager(), tabletSnapshot] {
            auto writer = CreateWireProtocolWriter();
            i64 dataWeight = 0;
            for (const auto& row : rows) {
                writer->WriteCommand(EWireProtocolCommand::VersionedWriteRow);
                writer->WriteVersionedRow(row);
                dataWeight += GetDataWeight(row);
            }
            auto wireData = writer->Finish();
            struct TTag { };
            auto reader = CreateWireProtocolReader(MergeRefsToRef<TTag>(wireData));

            TAuthenticationIdentity identity(ReplicatorUserName);
            TCurrentAuthenticationIdentityGuard guard(&identity);

            auto future = tabletWriteManager->Write(
                tabletSnapshot,
                reader.get(),
                TTabletCellWriteParams{
                    .TransactionId = transactionId,
                    .TransactionStartTimestamp = TimestampFromTransactionId(transactionId),
                    .TransactionTimeout = TDuration::Max(),
                    .PrepareSignature = signature,
                    .CommitSignature = signature,
                    .RowCount = static_cast<int>(std::ssize(rows)),
                    .DataWeight = dataWeight,
                    .Versioned = true
                });

            // NB: we are not going to return the future since it will be set only when
            // WriteRows mutation (or mutations) are applied; we are applying mutations
            // manually in these unittests, so this future is meaningless.
            // Still, it is useful to check that no error is thrown in WriteRows mutation handler.
            future
                .Subscribe(BIND([] (const TError& error) {
                    YT_VERIFY(error.IsOK());
                }));
        })
            .AsyncVia(AutomatonInvoker())
            .Run();
    }

    TFuture<void> PrepareTransactionCommit(TTransactionId transactionId, bool persistent, TTimestamp prepareTimestamp)
    {
        return TransactionSupervisor()->PrepareTransactionCommit(
            transactionId,
            persistent,
            prepareTimestamp);
    }

    TFuture<void> CommitTransaction(TTransactionId transactionId, TTimestamp commitTimestamp)
    {
        return TransactionSupervisor()->CommitTransaction(
            transactionId,
            commitTimestamp);
    }

    TFuture<void> PrepareAndCommitTransaction(TTransactionId transactionId, bool persistent, TTimestamp prepareAndCommitTimestamp)
    {
        auto asyncPrepare = PrepareTransactionCommit(transactionId, persistent, prepareAndCommitTimestamp);
        auto asyncCommit = CommitTransaction(transactionId, prepareAndCommitTimestamp);
        return AllSucceeded<void>({asyncPrepare, asyncCommit});
    }

    TFuture<void> AbortTransaction(TTransactionId transactionId, bool force)
    {
        return TransactionSupervisor()->AbortTransaction(
            transactionId,
            force);
    }

    void ExpectFullyUnlocked()
    {
        auto* tablet = TabletSlot_->TabletManager()->GetTablet();

        auto [lockCount, hasActiveLocks] = RunInAutomaton([&] {
            return std::pair(tablet->GetTotalTabletLockCount(), tablet->GetStoreManager()->HasActiveLocks());
        });

        EXPECT_EQ(0, lockCount);
        EXPECT_FALSE(hasActiveLocks);
    }

    bool HasActiveStoreLocks()
    {
        auto* tablet = TabletSlot_->TabletManager()->GetTablet();

        return RunInAutomaton([&] {
            return tablet->GetStoreManager()->HasActiveLocks();
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NYT::NTabletNode
