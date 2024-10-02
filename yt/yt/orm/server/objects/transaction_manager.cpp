#include "transaction_manager.h"

#include "transaction.h"
#include "object_manager.h"
#include "config.h"
#include "private.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>

#include <yt/yt/orm/server/master/bootstrap.h>
#include <yt/yt/orm/server/master/config.h>
#include <yt/yt/orm/server/master/helpers.h>
#include <yt/yt/orm/server/master/yt_connector.h>

#include <yt/yt/orm/client/objects/helpers.h>

#include <yt/yt/client/api/sticky_transaction_pool.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <util/system/guard.h>

#include <atomic>

namespace NYT::NOrm::NServer::NObjects {

using namespace NServer::NMaster;

using namespace NYT::NApi;
using namespace NYT::NTransactionClient;

using namespace NYT::NConcurrency;
using namespace NYT::NThreading;

using NClient::NObjects::InstanceTagFromTransactionId;
using NClient::NObjects::GenerateTransactionId;
using NClient::NObjects::ClusterTagFromId;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TTimestampBuffer
    : public TRefCounted
{
public:
    TTimestampBuffer(TTimestamp begin, ui64 watermark, ui64 length)
        : Current_(begin)
        , Watermark_(begin + length - watermark)
        , End_(begin + length)
    { }

    bool AtWatermark(TTimestamp timestamp) const
    {
        return timestamp == Watermark_;
    }

    bool Overrun(TTimestamp timestamp) const
    {
        return timestamp >= End_;
    }

    TTimestamp Get()
    {
        return Current_.fetch_add(1, std::memory_order::relaxed);
    }

private:
    std::atomic<TTimestamp> Current_;
    const TTimestamp Watermark_;
    const TTimestamp End_;
};

DECLARE_REFCOUNTED_CLASS(TTimestampBuffer)
DEFINE_REFCOUNTED_TYPE(TTimestampBuffer)

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTransactionManager* owner,
        IBootstrap* bootstrap,
        TTransactionManagerConfigPtr config)
        : Owner_(owner)
        , Bootstrap_(bootstrap)
        , TimestampBufferUpdateExecutor_(New<TPeriodicExecutor>(
            TimestampBufferUpdateQueue_->GetInvoker(),
            BIND(&TImpl::OnUpdateTimestampBuffer, MakeWeak(this)),
            config->TimestampBufferUpdatePeriod))
        , Profiler_(Profiler.WithPrefix("/transaction_manager"))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(
            TimestampBufferUpdateQueue_->GetInvoker(),
            TimestampBufferUpdateThread);
        Bootstrap_->SubscribeConfigUpdate(BIND(&TImpl::OnConfigUpdate, MakeWeak(this)));

        SetConfig(std::move(config));

        Profiler_.AddFuncGauge(
            "/concurrency",
            MakeStrong(this),
            [this] {
                auto guard = ReaderGuard(TransactionMapLock_);
                return TransactionMap_.size();
            });
    }

    void Initialize()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        ytConnector->SubscribeValidateConnection(BIND(&TImpl::OnValidateConnection, MakeWeak(this)));
        ytConnector->SubscribeConnected(BIND([&] {
            if (GetConfig()->TimestampBufferEnabled) {
                TimestampBufferUpdateExecutor_->Start();
            }
        }));
        ytConnector->SubscribeDisconnected(BIND([&] {
            Y_UNUSED(TimestampBufferUpdateExecutor_->Stop());
        }));
    }

    TTransactionManagerConfigPtr GetConfig() const
    {
        return Config_.Acquire();
    }

    TFuture<TTimestamp> GenerateTimestamps(int count)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        if (auto error = ytConnector->CheckConnected(); !error.IsOK()) {
            return MakeFuture<TTimestamp>(std::move(error));
        }
        const auto& client = ytConnector->GetClient(ytConnector->FormatUserTag());
        const auto& timestampProvider = client->GetTimestampProvider();
        return timestampProvider->GenerateTimestamps(count);
    }

    TTimestamp GenerateTimestampBuffered(bool allowRecursion)
    {
        const auto OnOverrun = [&] () -> TTimestamp {
            OverrunCounter_.Increment();
            if (allowRecursion) {
                TimestampBufferUpdateExecutor_->GetExecutedEvent().Wait();
                return GenerateTimestampBuffered(false);
            } else {
                THROW_ERROR_EXCEPTION("Cannot generate timestamp due to buffer being empty twice in a row");
            }
        };
        const auto config = GetConfig();
        auto buffer = TimestampBuffer_.Acquire();
        if (buffer == nullptr) {
            YT_LOG_WARNING("Timestamp buffer is not created yet");
            return OnOverrun();
        }
        const auto timestamp = buffer->Get();
        if (buffer->AtWatermark(timestamp)) {
            WatermarkCounter_.Increment();
            YT_LOG_WARNING("Timestamp buffer watermark reached");
            TimestampBufferUpdateExecutor_->ScheduleOutOfBand();
        }
        if (buffer->Overrun(timestamp)) {
            YT_LOG_WARNING("Timestamp buffer overrun");
            return OnOverrun();
        }
        return timestamp;
    }

    TFuture<TTransactionPtr> StartReadWriteTransaction(TStartReadWriteTransactionOptions options)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        if (auto error = ytConnector->CheckConnected(); !error.IsOK()) {
            return MakeFuture<TTransactionPtr>(std::move(error));
        }

        auto identityUserTag = ytConnector->FormatUserTag(std::move(options.UserTag));
        // It is crucial to create the client within this fiber
        // to use correct RPC authentication identity user tag,
        // which is stored in the fiber local storage.
        auto client = ytConnector->GetClient(identityUserTag);

        EYTTransactionOwnership underlyingTransactionOwnership = EYTTransactionOwnership::Owned;
        TFuture<ITransactionPtr> underlyingTransactionFuture;

        if (options.UnderlyingTransactionId != NullTransactionId) {
            if (options.UnderlyingTransactionAddress) {
                underlyingTransactionOwnership = EYTTransactionOwnership::NonOwnedAttached;
                underlyingTransactionFuture = BIND([client, options] {
                    TTransactionAttachOptions attachOptions;
                    attachOptions.StickyAddress = options.UnderlyingTransactionAddress;
                    return client->AttachTransaction(
                        options.UnderlyingTransactionId,
                        attachOptions);
                }).AsyncVia(GetCurrentInvoker()).Run();
            } else {
                underlyingTransactionOwnership = EYTTransactionOwnership::NonOwnedCollocated;
                auto underlyingTransaction = Bootstrap_->GetUnderlyingTransactionPool()->FindTransactionAndRenewLease(
                    options.UnderlyingTransactionId);
                if (underlyingTransaction) {
                    YT_LOG_DEBUG("Found underlying transaction within the internal pool (UnderlyingTransactionId: %v)",
                        options.UnderlyingTransactionId);
                    underlyingTransactionFuture = MakeFuture(std::move(underlyingTransaction));
                } else {
                    underlyingTransactionFuture = MakeFuture<ITransactionPtr>(TError(
                        "Underlying transaction %v is not found within the internal pool",
                        options.UnderlyingTransactionId));
                }
            }
        } else {
            if (options.UnderlyingTransactionAddress) {
                return MakeFuture<TTransactionPtr>(TError(
                    "Underlying transaction address %v is specified, but "
                    "underlying transaction id is missing",
                    options.UnderlyingTransactionAddress));
            }

            TTransactionStartOptions ytOptions;
            ytOptions.StartTimestamp = options.StartTimestamp;
            underlyingTransactionFuture = client->StartTransaction(ETransactionType::Tablet, ytOptions);
        }

        auto identity = NAccessControl::TryGetAuthenticatedUserIdentity().value_or(
            NRpc::GetRootAuthenticationIdentity());

        return underlyingTransactionFuture.ApplyUnique(BIND([
                this,
                this_ = MakeStrong(this),
                startTimestamp = options.StartTimestamp,
                leaseDuration = options.LeaseDuration,
                underlyingTransactionOwnership = underlyingTransactionOwnership,
                options = std::move(options.MutatingTransactionOptions),
                identityUserTag = std::move(identityUserTag),
                identity = std::move(identity),
                manager = Bootstrap_->GetAccessControlManager()
            ] (ITransactionPtr&& underlyingTransaction) {
                auto underlyingTimestamp = underlyingTransaction->GetStartTimestamp();
                // Start timestamp validation serves as:
                // * Sanity check for a new transaction;
                // * Input parameters validation for an attached or found in internal pool transaction.
                if (startTimestamp != NullTimestamp && startTimestamp != underlyingTimestamp) {
                    THROW_ERROR_EXCEPTION("Requested start timestamp %v "
                        "differs from the underlying transaction start timestamp %v",
                        startTimestamp,
                        underlyingTimestamp);
                }

                std::optional<NAccessControl::TAuthenticatedUserGuard> guard;
                if (manager->TryGetClusterSubjectSnapshot()) {
                    guard.emplace(manager, identity);
                }

                TimestampLag_.Update(TInstant::Now() - TimestampToInstant(underlyingTimestamp).first);
                return RegisterTransaction(
                    underlyingTransaction,
                    leaseDuration,
                    underlyingTransactionOwnership,
                    std::move(identityUserTag),
                    std::move(options));
            }));
    }

    TFuture<TTransactionPtr> StartReadOnlyTransaction(TStartReadOnlyTransactionOptions options)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        if (auto error = ytConnector->CheckConnected(); !error.IsOK()) {
            return MakeFuture<TTransactionPtr>(std::move(error));
        }

        auto identityUserTag = ytConnector->FormatUserTag(std::move(options.UserTag));
        // It is crucial to create the client within this fiber
        // to use correct rpc authentication identity user tag,
        // which is stored in the fiber local storage.
        auto client = ytConnector->GetClient(identityUserTag);

        auto identity = NAccessControl::TryGetAuthenticatedUserIdentity().value_or(
            NRpc::GetRootAuthenticationIdentity());

        auto onTimestampGenerated = [
            this,
            this_ = MakeStrong(this),
            client,
            options,
            identityUserTag = std::move(identityUserTag),
            identity = std::move(identity),
            manager = Bootstrap_->GetAccessControlManager()
        ] (TTimestamp&& startTimestamp) {
            std::optional<NAccessControl::TAuthenticatedUserGuard> guard;
            if (manager->TryGetClusterSubjectSnapshot()) {
                guard.emplace(manager, identity);
            }

            ValidateDataCompleteness(startTimestamp);

            auto id = MakeTransactionId(startTimestamp);
            auto transaction = Owner_->NewTransaction(
                TTransactionConfigsSnapshot{
                    Bootstrap_->GetObjectManager()->GetConfig(),
                    GetConfig()
                },
                id,
                startTimestamp,
                TYTClientDescriptor{std::move(client)},
                std::move(identityUserTag),
                std::move(options.ReadingTransactionOptions));

            // Read phase limit is used to track various linear lookups in update requests.
            // For read-only transactions it does not make a lot of sense to try limiting read phases.
            transaction->SetReadPhaseLimit(0);

            TimestampLag_.Update(TInstant::Now() - TimestampToInstant(startTimestamp).first);

            YT_LOG_DEBUG("Read-only transaction created (TransactionId: %v, StartTimestamp: %v)",
                id,
                startTimestamp);

            return transaction;
        };

        if (options.StartTimestamp == NullTimestamp) {
            YT_LOG_DEBUG("Generating transaction start timestamp");
            return client->GetTimestampProvider()->GenerateTimestamps().ApplyUnique(BIND(onTimestampGenerated));
        } else {
            return MakeFuture(onTimestampGenerated(std::move(options.StartTimestamp)));
        }
    }

    TTransactionPtr FindTransactionIfConnectedOrThrow(TTransactionId id)
    {
        Bootstrap_->GetYTConnector()->CheckConnected()
            .ThrowOnError();

        auto guard = ReaderGuard(TransactionMapLock_);
        auto it = TransactionMap_.find(id);
        if (it == TransactionMap_.end()) {
            return nullptr;
        }
        auto& entry = it->second;
        TLeaseManager::RenewLease(entry.Lease);

        return entry.Transaction;
    }

    TTransactionPtr GetTransactionIfConnectedOrThrow(TTransactionId id)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        if (ClusterTagFromId(id) != ytConnector->GetClusterTag()) {
            THROW_ERROR_EXCEPTION(
                "Invalid cluster tag in transaction id %v: expected %v, got %v",
                id,
                ytConnector->GetClusterTag(),
                ClusterTagFromId(id));
        }
        auto instanceTag = GetInstanceTagOrThrow();
        if (InstanceTagFromTransactionId(id) != instanceTag) {
            THROW_ERROR_EXCEPTION(
                "Invalid master instance tag in transaction id %v: expected %v, got %v. "
                "Try addressing master which started the transaction",
                id,
                instanceTag,
                InstanceTagFromTransactionId(id));
        }
        auto transaction = FindTransactionIfConnectedOrThrow(id);
        if (!transaction) {
            THROW_ERROR_EXCEPTION(
                NClient::EErrorCode::NoSuchTransaction,
                "No such transaction %v",
                id);
        }
        return transaction;
    }

    void ValidateDataCompleteness(TTimestamp timestamp) const
    {
        auto databaseFinalizationTimestamp = DatabaseFinalizationTimestamp_.load();

        if (databaseFinalizationTimestamp == NullTimestamp) {
            THROW_ERROR_EXCEPTION(
                "Database finalization timestamp is not available yet, "
                "so it is impossible to guarantee data completeness for the given timestamp");
        }

        if (GetConfig()->ValidateDatabaseTimestamp && timestamp < databaseFinalizationTimestamp) {
            THROW_ERROR_EXCEPTION(NClient::EErrorCode::TimestampOutOfRange,
                "Timestamp %v is less than database finalization timestamp %v, "
                "so it is impossible to guarantee data completeness for the given timestamp",
                timestamp,
                databaseFinalizationTimestamp);
        }
    }

    void ProfilePerformanceStatistics(const TPerformanceStatistics& statistics)
    {
        ReadPhaseCountSummary_.Record(statistics.ReadPhaseCount);
    }

    //! Default implementation, may be overridden
    //! by the derived data-model-specific transaction managers.
    //! Use Owner_->NewTransaction to create new transaction virtually.
    TTransactionPtr NewTransactionImpl(
        TTransactionConfigsSnapshot configsSnapshot,
        TTransactionId id,
        TTimestamp startTimestamp,
        TYTTransactionOrClientDescriptor ytTransactionOrClient,
        std::string identityUserTag,
        TTransactionOptions options)
    {
        return New<TTransaction>(
            Bootstrap_,
            std::move(configsSnapshot),
            id,
            startTimestamp,
            std::move(ytTransactionOrClient),
            std::move(identityUserTag),
            std::move(options));
    }

private:
    TTransactionManager* const Owner_;
    IBootstrap* const Bootstrap_;

    const TActionQueuePtr TimestampBufferUpdateQueue_ = New<TActionQueue>("TimestampBuffer");
    const TPeriodicExecutorPtr TimestampBufferUpdateExecutor_;
    const NProfiling::TProfiler Profiler_;

    const NProfiling::TCounter WatermarkCounter_ =
        Profiler_.Counter("/timestamp_buffer/watermark_count");
    const NProfiling::TCounter OverrunCounter_ =
        Profiler_.Counter("/timestamp_buffer/overrun_count");
    const NProfiling::TCounter ErrorCounter_ =
        Profiler_.Counter("/timestamp_buffer/error_count");
    const NProfiling::TSummary ReadPhaseCountSummary_ =
        Profiler_.WithHot().Summary("/read_phase_count");
    const NProfiling::TTimeGauge TimestampLag_ =
        Profiler_.WithHot().TimeGaugeSummary("/timestamp_lag");

    TAtomicIntrusivePtr<TTransactionManagerConfig> Config_;

    struct TTransactionEntry
    {
        TTransactionPtr Transaction;
        TLease Lease;
    };

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, TransactionMapLock_);
    THashMap<TTransactionId, TTransactionEntry> TransactionMap_;

    std::atomic<TTimestamp> DatabaseFinalizationTimestamp_{NullTimestamp};

    TAtomicIntrusivePtr<TTimestampBuffer> TimestampBuffer_;

    DECLARE_THREAD_AFFINITY_SLOT(TimestampBufferUpdateThread);

    TMasterInstanceTag GetInstanceTagOrThrow()
    {
        auto tag = Bootstrap_->GetYTConnector()->GetInstanceTag();
        if (tag == NClient::NObjects::UndefinedMasterInstanceTag) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable,
                "Master is not connected to YT: undefined instance tag");
        }
        return tag;
    }

    void SetConfig(TTransactionManagerConfigPtr config)
    {
        Config_.Store(config);
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        if (config->TimestampBufferEnabled) {
            if (ytConnector->IsConnected()) {
                TimestampBufferUpdateExecutor_->Start();
            }
        } else {
            YT_UNUSED_FUTURE(TimestampBufferUpdateExecutor_->Stop());
        }
        TimestampBufferUpdateExecutor_->SetPeriod(config->TimestampBufferUpdatePeriod);
    }

    void OnConfigUpdate(const TMasterDynamicConfigPtr& config)
    {
        if (NMaster::AreConfigsEqual(GetConfig(), config->TransactionManager)) {
            return;
        }

        YT_LOG_INFO("Updating transaction manager configuration");
        SetConfig(config->TransactionManager);
    }

    void OnValidateConnection()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto& client = ytConnector->GetClient(ytConnector->FormatUserTag());

        try {
            auto ysonTimestamp = WaitFor(client->GetNode(ytConnector->GetDBPath() + "/@finalization_timestamp"))
                .ValueOrThrow();
            auto timestamp = NYTree::ConvertTo<TTimestamp>(ysonTimestamp);
            {
                TTimestamp expectedTimestamp = NullTimestamp;
                if (!DatabaseFinalizationTimestamp_.compare_exchange_strong(expectedTimestamp, timestamp) &&
                    GetConfig()->ValidateDatabaseTimestamp)
                {
                    // Timestamp change means database change almost surely which is forbidden for now
                    // while the instance is active.
                    YT_VERIFY(expectedTimestamp == timestamp);
                }
            }
            YT_LOG_DEBUG("Got database finalization timestamp (FinalizationTimestamp: %v)",
                timestamp);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error getting database finalization timestamp")
                << ex;
        }
    }

    TTransactionId MakeTransactionId(TTimestamp timestamp)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        auto instanceTag = GetInstanceTagOrThrow();
        return GenerateTransactionId(instanceTag, ytConnector->GetClusterTag(), timestamp);
    }

    TTransactionPtr RegisterTransaction(
        const ITransactionPtr& underlyingTransaction,
        TDuration leaseDuration,
        EYTTransactionOwnership underlyingTransactionOwnership,
        std::string identityUserTag,
        TMutatingTransactionOptions options)
    {
        auto startTimestamp = underlyingTransaction->GetStartTimestamp();
        auto id = MakeTransactionId(startTimestamp);

        ValidateDataCompleteness(startTimestamp);

        TYTTransactionDescriptor transactionDescriptor{
            underlyingTransaction,
            underlyingTransactionOwnership
        };

        auto transaction = Owner_->NewTransaction(
            TTransactionConfigsSnapshot{
                Bootstrap_->GetObjectManager()->GetConfig(),
                GetConfig()
            },
            id,
            startTimestamp,
            transactionDescriptor,
            std::move(identityUserTag),
            std::move(options));

        SubscribeCommitted(
            transactionDescriptor,
            BIND(&TImpl::OnTransactionCommitted, MakeWeak(this), id));
        SubscribeAborted(
            transactionDescriptor,
            BIND(&TImpl::OnTransactionAborted, MakeWeak(this), id));

        if (leaseDuration == TDuration::Zero()) {
            leaseDuration = underlyingTransaction->GetTimeout();
        }

        {
            auto guard = WriterGuard(TransactionMapLock_);
            auto lease = TLeaseManager::CreateLease(
                leaseDuration,
                BIND(&TImpl::OnTransactionLeaseExpired, MakeWeak(this), id));
            TTransactionEntry entry{transaction, lease};
            if (!TransactionMap_.emplace(id, entry).second) {
                THROW_ERROR_EXCEPTION("Transaction %v is already registered",
                    id);
            }
        }

        YT_LOG_DEBUG("Read-write transaction registered ("
            "TransactionId: %v, "
            "StartTimestamp: %v, "
            "UnderlyingTransactionId: %v, "
            "UnderlyingTransactionOwnership: %v)",
            id,
            startTimestamp,
            underlyingTransaction->GetId(),
            underlyingTransactionOwnership);

        return transaction;
    }

    void OnTransactionCommitted(TTransactionId id) noexcept
    {
        UnregisterTransaction(id);
    }

    void OnTransactionAborted(TTransactionId id, const TError& /*error*/) noexcept
    {
        UnregisterTransaction(id);
    }

    void UnregisterTransaction(TTransactionId id) noexcept
    {
        if (auto transaction = EraseTransaction(id, /*closeLease*/ true)) {
            YT_LOG_DEBUG("Read-write transaction unregistered (TransactionId: %v)",
                id);
        }
    }

    void OnTransactionLeaseExpired(TTransactionId id) noexcept
    {
        if (auto transaction = EraseTransaction(id, /*closeLease*/ false)) {
            YT_LOG_DEBUG("Read-write transaction lease expired (TransactionId: %v)",
                id);

            // Fire-and-forget.
            YT_UNUSED_FUTURE(transaction->Abort());
        }
    }

    TTransactionPtr EraseTransaction(TTransactionId id, bool closeLease) noexcept
    {
        TTransactionPtr transaction;
        auto guard = WriterGuard(TransactionMapLock_);
        auto it = TransactionMap_.find(id);
        if (it == TransactionMap_.end()) {
            return transaction;
        }
        auto& entry = it->second;
        if (closeLease) {
            TLeaseManager::CloseLease(entry.Lease);
        }
        transaction = std::move(entry.Transaction);
        TransactionMap_.erase(it);
        return transaction;
    }

    void OnUpdateTimestampBuffer()
    {
        VERIFY_THREAD_AFFINITY(TimestampBufferUpdateThread);
        try {
            const auto config = GetConfig();
            const auto timestampCount = config->TimestampBufferSize;
            const auto watermark = config->TimestampBufferLowWatermark;
            const auto& ytConnector = Bootstrap_->GetYTConnector();
            const auto& client = ytConnector->GetClient(ytConnector->FormatUserTag());
            const auto& timestampProvider = client->GetTimestampProvider();
            TimestampBuffer_.Store(New<TTimestampBuffer>(
                WaitFor(timestampProvider->GenerateTimestamps(timestampCount))
                    .ValueOrThrow(),
                watermark,
                timestampCount));
        } catch (const std::exception& ex) {
            ErrorCounter_.Increment();
            YT_LOG_WARNING(ex, "Timestamp buffer update failed");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(
    IBootstrap* bootstrap,
    TTransactionManagerConfigPtr config)
    : Impl_(New<TImpl>(
        this,
        bootstrap,
        std::move(config)))
{ }

TTransactionManager::~TTransactionManager()
{ }

void TTransactionManager::Initialize()
{
    Impl_->Initialize();
}

TTransactionManagerConfigPtr TTransactionManager::GetConfig() const
{
    return Impl_->GetConfig();
}

TFuture<TTimestamp> TTransactionManager::GenerateTimestamps(int count)
{
    return Impl_->GenerateTimestamps(count);
}

TTimestamp TTransactionManager::GenerateTimestampBuffered()
{
    return Impl_->GenerateTimestampBuffered(true);
}

TFuture<TTransactionPtr> TTransactionManager::StartReadWriteTransaction(
    TStartReadWriteTransactionOptions options)
{
    return Impl_->StartReadWriteTransaction(
        std::move(options));
}

TFuture<TTransactionPtr> TTransactionManager::StartReadOnlyTransaction(
    TStartReadOnlyTransactionOptions options)
{
    return Impl_->StartReadOnlyTransaction(
        std::move(options));
}

TTransactionPtr TTransactionManager::FindTransactionIfConnectedOrThrow(TTransactionId id)
{
    return Impl_->FindTransactionIfConnectedOrThrow(id);
}

TTransactionPtr TTransactionManager::GetTransactionIfConnectedOrThrow(TTransactionId id)
{
    return Impl_->GetTransactionIfConnectedOrThrow(id);
}

void TTransactionManager::ValidateDataCompleteness(TTimestamp timestamp) const
{
    Impl_->ValidateDataCompleteness(timestamp);
}

void TTransactionManager::ProfilePerformanceStatistics(const TPerformanceStatistics& statistics)
{
    Impl_->ProfilePerformanceStatistics(statistics);
}

////////////////////////////////////////////////////////////////////////////////

TTransactionPtr TTransactionManager::NewTransaction(
    TTransactionConfigsSnapshot configsSnapshot,
    TTransactionId id,
    TTimestamp startTimestamp,
    TYTTransactionOrClientDescriptor ytTransactionOrClient,
    std::string identityUserTag,
    TTransactionOptions options)
{
    // Default implementation.
    return Impl_->NewTransactionImpl(
        std::move(configsSnapshot),
        id,
        startTimestamp,
        std::move(ytTransactionOrClient),
        std::move(identityUserTag),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
