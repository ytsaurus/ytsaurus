#include "election_manager.h"

#include "private.h"
#include "config.h"

#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>
#include <yt/yt/core/concurrency/context_switch.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NChaosElection {

using namespace NApi;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

inline static constexpr TStringBuf LockKeyColumn = "lock_key";
inline static constexpr TStringBuf LeaderLeaseIdColumn = "leader_lease_id";
inline static constexpr TStringBuf LeaderNameColumn = "leader_name";
inline static constexpr TStringBuf LeaseTimeoutColumn = "lease_timeout";
inline static constexpr TStringBuf LastPingTimeColumn = "last_ping_time";

TTableSchemaPtr GetChaosElectionLockTableSchema()
{
    return New<TTableSchema>(std::vector<TColumnSchema>{
        TColumnSchema(TString(LockKeyColumn), EValueType::String).SetSortOrder(ESortOrder::Ascending),
        TColumnSchema(TString(LeaderLeaseIdColumn), EValueType::String),
        TColumnSchema(TString(LeaderNameColumn), EValueType::String),
        TColumnSchema(TString(LeaseTimeoutColumn), EValueType::Uint64),
        TColumnSchema(TString(LastPingTimeColumn), EValueType::Uint64),
    });
}

////////////////////////////////////////////////////////////////////////////////

class TChaosElectionManager
    : public NLockElection::ILockElectionManager
{
public:
    TChaosElectionManager(
        IClientPtr client,
        IInvokerPtr invoker,
        TChaosElectionManagerConfigPtr config,
        TChaosElectionManagerOptionsPtr options)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Client_(std::move(client))
        , Invoker_(CreateSerializedInvoker(
            std::move(invoker),
            NProfiling::TTagSet({
                {"invoker", "chaos_election_manager"},
                {"group", Options_->GroupName},
                {"path", Config_->LockTablePath},
            })))
        , Logger(ChaosElectionLogger().WithTag(
            "GroupName: %v, Path: %v",
            Options_->GroupName,
            Config_->LockTablePath))
        , LockAcquisitionExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TChaosElectionManager::TryAcquireLock, MakeWeak(this)),
            Config_->LockAcquisitionPeriod))
        , LeasePingExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TChaosElectionManager::PingLease, MakeWeak(this)),
            Config_->LeasePingPeriod))
    { }

    void Start() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Starting chaos election manager");

        IsActive_ = true;

        LockAcquisitionExecutor_->Start();
    }

    TFuture<void> Stop() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Stopping chaos election manager");

        return BIND(&TChaosElectionManager::DoStop, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    bool IsActive() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return IsActive_;
    }

    TFuture<void> StopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Stopping leading");

        return BIND(&TChaosElectionManager::DoStopLeading, MakeWeak(this))
            .AsyncVia(Invoker_)
            .Run();
    }

    NPrerequisiteClient::TPrerequisiteId GetPrerequisiteId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return PrerequisiteId_.Load();
    }

    bool IsLeader() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return GetPrerequisiteId() != NPrerequisiteClient::TPrerequisiteId{};
    }

    DEFINE_SIGNAL_OVERRIDE(void(), LeadingStarted);
    DEFINE_SIGNAL_OVERRIDE(void(), LeadingEnded);

private:
    const TChaosElectionManagerConfigPtr Config_;
    const TChaosElectionManagerOptionsPtr Options_;
    const IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const TLogger Logger;

    const TPeriodicExecutorPtr LockAcquisitionExecutor_;
    const TPeriodicExecutorPtr LeasePingExecutor_;

    IPrerequisitePtr Lease_;

    NThreading::TAtomicObject<NPrerequisiteClient::TPrerequisiteId> PrerequisiteId_;

    std::atomic<bool> IsActive_ = false;

    //  NB: Lock acquisition involves WaitFor calls inside a serialized invoker.
    //      A reentrancy guard prevents overlapping acquisition attempts when the
    //      periodic executor fires while the previous attempt is still yielded.
    bool AcquireLockInProgress_ = false;

    void TryAcquireLock()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (IsLeader() || AcquireLockInProgress_) {
            return;
        }

        AcquireLockInProgress_ = true;
        auto guard = Finally([&] {
            AcquireLockInProgress_ = false;
        });

        try {
            GuardedTryAcquireLock();
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Lock acquisition iteration failed");
        }
    }

    void GuardedTryAcquireLock()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();

        auto nameTable = New<TNameTable>();
        auto lockKeyColumnId = nameTable->RegisterName(LockKeyColumn);
        auto leaderLeaseIdColumnId = nameTable->RegisterName(LeaderLeaseIdColumn);
        auto leaderNameColumnId = nameTable->RegisterName(LeaderNameColumn);
        auto leaseTimeoutColumnId = nameTable->RegisterName(LeaseTimeoutColumn);
        auto lastPingTimeColumnId = nameTable->RegisterName(LastPingTimeColumn);

        auto rowBuffer = New<TRowBuffer>();

        TUnversionedRowBuilder keyBuilder;
        keyBuilder.AddValue(MakeUnversionedStringValue(Options_->GroupName, lockKeyColumnId));

        auto key = rowBuffer->CaptureRow(keyBuilder.GetRow());

        auto lookupResult = WaitFor(transaction->LookupRows(
            Config_->LockTablePath,
            nameTable,
            MakeSharedRange(std::vector<TUnversionedRow>{key}, rowBuffer)))
            .ValueOrThrow();

        auto rows = lookupResult.Rowset->GetRows();
        auto resultNameTable = lookupResult.Rowset->GetNameTable();

        if (!rows.Empty()) {
            auto row = rows[0];
            auto leaseIdColumnId = resultNameTable->GetIdOrThrow(LeaderLeaseIdColumn);
            auto timeoutColumnId = resultNameTable->GetIdOrThrow(LeaseTimeoutColumn);
            auto pingTimeColumnId = resultNameTable->GetIdOrThrow(LastPingTimeColumn);

            auto existingLeaseId = FromUnversionedValue<std::optional<TChaosLeaseId>>(row[leaseIdColumnId]);
            auto leaseTimeout = FromUnversionedValue<std::optional<TDuration>>(row[timeoutColumnId]);
            auto lastPingTime = FromUnversionedValue<std::optional<TInstant>>(row[pingTimeColumnId]);

            if (existingLeaseId) {
                bool timeoutIsActive = leaseTimeout
                  && lastPingTime
                  && TInstant::Now() < *lastPingTime + *leaseTimeout;

                if (timeoutIsActive) {
                    YT_LOG_DEBUG(
                        "Existing leader lease timeout is still active, backing off "
                        "(LeaseId: %v, LastPingTime: %v, LeaseTimeout: %v)",
                        existingLeaseId,
                        lastPingTime,
                        leaseTimeout);
                    WaitFor(transaction->Abort())
                        .ThrowOnError();
                    return;
                }

                try {
                    auto existingLease = WaitFor(Client_->AttachChaosLease(*existingLeaseId))
                        .ValueOrThrow();

                    YT_LOG_DEBUG("Existing leader lease is alive (LeaseId: %v)",
                        existingLeaseId);

                    WaitFor(transaction->Abort())
                        .ThrowOnError();

                    return;
                } catch (const TErrorException& ex) {
                    if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
                        YT_LOG_DEBUG("Existing leader lease is dead, attempting takeover (LeaseId: %v)",
                            existingLeaseId);
                    } else {
                        throw;
                    }
                }
            }
        }

        auto chaosLeaseId = CreateLeaseOnEnabledCell();

        YT_LOG_DEBUG("Created chaos lease (LeaseId: %v)", chaosLeaseId);

        TChaosLeaseAttachOptions leaseAttachOptions;
        leaseAttachOptions.Ping = true;
        auto now = TInstant::Now();

        auto lease = WaitFor(Client_->AttachChaosLease(chaosLeaseId, leaseAttachOptions))
            .ValueOrThrow();

        TUnversionedOwningRowBuilder rowBuilder;
        rowBuilder.AddValue(ToUnversionedValue(Options_->GroupName, rowBuffer, lockKeyColumnId));
        rowBuilder.AddValue(ToUnversionedValue(lease->GetId(), rowBuffer, leaderLeaseIdColumnId));
        rowBuilder.AddValue(ToUnversionedValue(Options_->MemberName, rowBuffer, leaderNameColumnId));
        rowBuilder.AddValue(ToUnversionedValue(Config_->LeaseTimeout, rowBuffer, leaseTimeoutColumnId));
        rowBuilder.AddValue(ToUnversionedValue(now, rowBuffer, lastPingTimeColumnId));
        auto owningRow = rowBuilder.FinishRow();

        std::vector<TUnversionedRow> writeRows;
        writeRows.push_back(owningRow);

        transaction->WriteRows(
            Config_->LockTablePath,
            nameTable,
            MakeSharedRange(std::move(writeRows)));

        auto commitResultOrError = WaitFor(transaction->Commit());
        if (!commitResultOrError.IsOK()) {
            YT_LOG_DEBUG(commitResultOrError, "Lock acquisition commit failed, will retry");
            return;
        }

        YT_LOG_DEBUG("Lock acquisition committed successfully (LeaseId: %v)",
            lease->GetId());

        Lease_ = std::move(lease);

        auto leaseId = Lease_->GetId();
        Lease_->SubscribeAborted(
            BIND(&TChaosElectionManager::OnLeaseAborted, MakeWeak(this), leaseId)
                .Via(Invoker_));

        LeasePingExecutor_->Start();

        OnLeadingStarted();
    }

    std::vector<TCellId> FetchMetadataCellIds()
    {
        auto path = Format("//sys/chaos_cell_bundles/%v/@metadata_cell_ids",
            Config_->ChaosCellBundle);
        auto result = WaitFor(Client_->GetNode(path))
            .ValueOrThrow();
        return ConvertTo<std::vector<TCellId>>(result);
    }

    TChaosLeaseId CreateLeaseOnEnabledCell()
    {
        auto cellIds = FetchMetadataCellIds();

        for (auto cellId : cellIds) {
            try {
                auto leaseAttributes = CreateEphemeralAttributes();
                leaseAttributes->Set("chaos_cell_id", cellId);
                leaseAttributes->Set("timeout", Config_->LeaseTimeout);

                TCreateObjectOptions createLeaseOptions;
                createLeaseOptions.Attributes = std::move(leaseAttributes);
                return WaitFor(Client_->CreateObject(EObjectType::ChaosLease, createLeaseOptions))
                    .ValueOrThrow();
            } catch (const TErrorException& ex) {
                if (ex.Error().FindMatching(NChaosClient::EErrorCode::ChaosCellIsNotEnabled)) {
                    YT_LOG_DEBUG("Chaos cell is not enabled, trying next (CellId: %v)",
                        cellId);
                    continue;
                }
                throw;
            }
        }

        THROW_ERROR_EXCEPTION("No enabled chaos cell found in bundle %Qv",
            Config_->ChaosCellBundle);
    }

    void PingLease()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (!IsLeader() || !Lease_) {
            return;
        }

        try {
            auto pingTime = TInstant::Now();
            WaitFor(Lease_->Ping())
                .ThrowOnError();

            UpdateLastPingTime(pingTime);

            YT_LOG_DEBUG("Chaos lease pinged successfully");
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to ping chaos lease");
        }
    }

    void UpdateLastPingTime(TInstant pingTime)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(Lease_);

        auto leaseId = Lease_->GetId();

        try {
            TTransactionStartOptions transactionOptions;
            auto transaction = WaitFor(
                Client_->StartTransaction(ETransactionType::Tablet, transactionOptions))
                .ValueOrThrow();

            auto lookupNameTable = New<TNameTable>();
            auto lookupKeyId = lookupNameTable->RegisterName(LockKeyColumn);

            auto rowBuffer = New<TRowBuffer>();
            TUnversionedRowBuilder keyBuilder;
            keyBuilder.AddValue(MakeUnversionedStringValue(Options_->GroupName, lookupKeyId));
            auto key = rowBuffer->CaptureRow(keyBuilder.GetRow());

            auto lookupResult = WaitFor(transaction->LookupRows(
                Config_->LockTablePath,
                lookupNameTable,
                MakeSharedRange(std::vector<TUnversionedRow>{key}, rowBuffer)))
                .ValueOrThrow();

            auto rows = lookupResult.Rowset->GetRows();
            auto resultNameTable = lookupResult.Rowset->GetNameTable();

            std::optional<TChaosLeaseId> rowLeaseId;
            if (!rows.Empty()) {
                auto leaseIdColumnId = resultNameTable->GetIdOrThrow(LeaderLeaseIdColumn);
                rowLeaseId = FromUnversionedValue<std::optional<TChaosLeaseId>>(rows[0][leaseIdColumnId]);
            }

            if (!rowLeaseId || *rowLeaseId != leaseId) {
                YT_LOG_WARNING(
                    "Lock row no longer references our lease, dropping leadership "
                    "(OurLeaseId: %v, RowLeaseId: %v)",
                    leaseId,
                    rowLeaseId);
                WaitFor(transaction->Abort())
                    .ThrowOnError();
                YT_UNUSED_FUTURE(Lease_->Abort());
                return;
            }

            auto writeNameTable = New<TNameTable>();
            auto lockKeyId = writeNameTable->RegisterName(LockKeyColumn);
            auto lastPingTimeId = writeNameTable->RegisterName(LastPingTimeColumn);

            TUnversionedOwningRowBuilder rowBuilder;
            rowBuilder.AddValue(ToUnversionedValue(Options_->GroupName, rowBuffer, lockKeyId));
            rowBuilder.AddValue(ToUnversionedValue(pingTime, rowBuffer, lastPingTimeId));
            auto owningRow = rowBuilder.FinishRow();

            std::vector<TUnversionedRow> writeRows;
            writeRows.push_back(owningRow);

            transaction->WriteRows(
                Config_->LockTablePath,
                writeNameTable,
                MakeSharedRange(std::move(writeRows)));

            WaitFor(transaction->Commit())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to update last ping time in lock table");
        }
    }

    void OnLeaseAborted(TObjectId leaseId, const TError& error)
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        YT_LOG_DEBUG(error, "Chaos lease aborted (LeaseId: %v)",
            leaseId);

        if (!Lease_ || Lease_->GetId() != leaseId) {
            return;
        }

        Reset();
    }

    void OnLeadingStarted()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);
        YT_VERIFY(!IsLeader());
        YT_VERIFY(Lease_);

        PrerequisiteId_.Store(Lease_->GetId());

        YT_LOG_DEBUG("Leading started (LeaseId: %v)",
            Lease_->GetId());

        try {
            TForbidContextSwitchGuard guard;
            LeadingStarted_.Fire();
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Unexpected error occurred during leading start");
        }
    }

    void DoStop()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        WaitFor(LockAcquisitionExecutor_->Stop())
            .ThrowOnError();
        WaitFor(LeasePingExecutor_->Stop())
            .ThrowOnError();

        Reset();

        IsActive_ = false;

        YT_LOG_DEBUG("Election manager stopped");
    }

    void DoStopLeading()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (IsLeader()) {
            Reset();
        }
    }

    void Reset()
    {
        YT_ASSERT_INVOKER_AFFINITY(Invoker_);

        if (IsLeader()) {
            YT_LOG_DEBUG("Leading ended");

            PrerequisiteId_.Store(NPrerequisiteClient::TPrerequisiteId{});

            try {
                TForbidContextSwitchGuard guard;
                LeadingEnded_.Fire();
            } catch (const std::exception& ex) {
                YT_LOG_ALERT(ex, "Unexpected error occurred during leading end");
            }
        }

        if (Lease_) {
            YT_UNUSED_FUTURE(Lease_->Abort());
            Lease_.Reset();
        }

        WaitFor(LeasePingExecutor_->Stop())
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

NLockElection::ILockElectionManagerPtr CreateChaosElectionManager(
    IClientPtr client,
    IInvokerPtr invoker,
    TChaosElectionManagerConfigPtr config,
    TChaosElectionManagerOptionsPtr options)
{
    return New<TChaosElectionManager>(
        std::move(client),
        std::move(invoker),
        std::move(config),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosElection
