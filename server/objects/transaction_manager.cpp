#include "transaction_manager.h"
#include "transaction.h"
#include "object_manager.h"
#include "object.h"
#include "config.h"
#include "private.h"

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/transaction.h>

#include <yt/ytlib/transaction_client/timestamp_provider.h>

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/lease_manager.h>

namespace NYP {
namespace NServer {
namespace NObjects {

using namespace NServer::NMaster;
using namespace NYT::NTransactionClient;
using namespace NYT::NApi;
using namespace NYT::NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTransactionManager::TImpl
    : public TRefCounted
{
public:
    TImpl(TBootstrap* bootstrap, TTransactionManagerConfigPtr config)
        : Bootstrap_(bootstrap)
        , Config_(std::move(config))
    { }

    TFuture<TTimestamp> GenerateTimestamp()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto& client = ytConnector->GetClient();
        const auto& connection = client->GetConnection();
        const auto& timestampProvider = connection->GetTimestampProvider();
        return timestampProvider->GenerateTimestamps(1);
    }

    TFuture<TTransactionPtr> StartReadWriteTransaction()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        const auto& client = ytConnector->GetClient();
        return client->StartTransaction(ETransactionType::Tablet)
            .Apply(BIND(&TImpl::RegisterTransaction, MakeStrong(this)));
    }

    TFuture<TTransactionPtr> StartReadOnlyTransaction(TTimestamp startTimestamp)
    {
        auto onTimestampGenerated = [=, this_ = MakeStrong(this)] (TTimestamp startTimestamp) {
            const auto& ytConnector = Bootstrap_->GetYTConnector();
            auto id = MakeTransactionId(startTimestamp);
            auto transaction = New<TTransaction>(
                Bootstrap_,
                Config_,
                id,
                startTimestamp,
                ytConnector->GetClient(),
                nullptr);
            LOG_DEBUG("Read-only transaction created (TransactionId: %v, StartTimestamp: %llx)",
                id,
                startTimestamp);
            return transaction;
        };

        if (startTimestamp == NullTimestamp) {
            LOG_DEBUG("Generating transaction start timestamp");
            const auto& ytConnector = Bootstrap_->GetYTConnector();
            const auto& timestampProvider = ytConnector->GetClient()->GetConnection()->GetTimestampProvider();
            return timestampProvider->GenerateTimestamps().Apply(BIND(onTimestampGenerated));
        } else {
            return MakeFuture(onTimestampGenerated(startTimestamp));
        }
    }

    TTransactionPtr FindTransaction(const TTransactionId& id)
    {
        TReaderGuard guard(TransactionMapLock_);
        auto it = TransactionMap_.find(id);
        if (it == TransactionMap_.end()) {
            return nullptr;
        }
        auto& entry = it->second;
        TLeaseManager::RenewLease(entry.Lease);
        return entry.Transaction;
    }

    TTransactionPtr GetTransactionOrThrow(const TTransactionId& id)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        if (ClusterTagFromId(id) != ytConnector->GetClusterTag()) {
            THROW_ERROR_EXCEPTION("Invalid cluster tag in transaction id %v: expected %v, got %v",
                ytConnector->GetClusterTag(),
                ClusterTagFromId(id));
        }
        if (MasterInstanceTagFromId(id) != ytConnector->GetInstanceTag()) {
            THROW_ERROR_EXCEPTION("Invalid master instance tag in transaction id %v: expected %v, got %v",
                ytConnector->GetInstanceTag(),
                MasterInstanceTagFromId(id));
        }
        auto transaction = FindTransaction(id);
        if (!transaction) {
            THROW_ERROR_EXCEPTION("No such transaction %v",
                id);
        }
        return transaction;
    }

private:
    TBootstrap* const Bootstrap_;
    const TTransactionManagerConfigPtr Config_;

    const NLogging::TLogger& Logger = NObjects::Logger;

    struct TTransactionEntry
    {
        TTransactionPtr Transaction;
        TLease Lease;
    };

    TReaderWriterSpinLock TransactionMapLock_;
    THashMap<TTransactionId, TTransactionEntry> TransactionMap_;


    TTransactionId MakeTransactionId(TTimestamp timestamp)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return TTransactionId(
            RandomNumber<ui32>(),
            (ytConnector->GetInstanceTag() << 24) + (ytConnector->GetClusterTag() << 16) + (1U << 14),
            static_cast<ui32>(timestamp & 0xffffffff),
            static_cast<ui32>(timestamp >> 32));
    }

    TTransactionPtr RegisterTransaction(const ITransactionPtr& underlyingTransaction)
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();

        auto startTimestamp = underlyingTransaction->GetStartTimestamp();
        auto id = MakeTransactionId(startTimestamp);

        auto transaction = New<TTransaction>(
            Bootstrap_,
            Config_,
            id,
            startTimestamp,
            ytConnector->GetClient(),
            underlyingTransaction);

        underlyingTransaction->SubscribeCommitted(BIND(&TImpl::UnregisterTransaction, MakeWeak(this), id));
        underlyingTransaction->SubscribeAborted(BIND(&TImpl::UnregisterTransaction, MakeWeak(this), id));

        {
            TWriterGuard guard(TransactionMapLock_);
            TTransactionEntry entry{
                transaction,
                TLeaseManager::CreateLease(
                    underlyingTransaction->GetTimeout(),
                    BIND(&TImpl::OnTransactionLeaseExpired, MakeWeak(this), id))
            };
            if (!TransactionMap_.emplace(id, entry).second) {
                THROW_ERROR_EXCEPTION("Transaction %v is already registered",
                    id);
            }
        }

        LOG_DEBUG("Read-write transaction registered (TransactionId: %v, StartTimestamp: %llx, UnderlyingTransactionId: %v)",
            id,
            startTimestamp,
            underlyingTransaction->GetId());

        return transaction;
    }

    void UnregisterTransaction(const TTransactionId& id)
    {
        {
            TWriterGuard guard(TransactionMapLock_);
            auto it = TransactionMap_.find(id);
            if (it == TransactionMap_.end()) {
                return;
            }
            auto& entry = it->second;
            TLeaseManager::CloseLease(entry.Lease);
            TransactionMap_.erase(it);
        }

        LOG_DEBUG("Read-write transaction unregistered (TransactionId: %v)",
            id);
    }

    void OnTransactionLeaseExpired(const TTransactionId& id)
    {
        TTransactionPtr transaction;
        {
            TWriterGuard guard(TransactionMapLock_);
            auto it = TransactionMap_.find(id);
            if (it == TransactionMap_.end()) {
                return;
            }
            auto& entry = it->second;
            transaction = std::move(entry.Transaction);
            TransactionMap_.erase(it);
        }

        LOG_DEBUG("Read-write transaction lease expired (TransactionId: %v)",
            id);

        transaction->Abort();
    }
};

////////////////////////////////////////////////////////////////////////////////

TTransactionManager::TTransactionManager(TBootstrap* bootstrap, TTransactionManagerConfigPtr config)
    : Impl_(New<TImpl>(bootstrap, std::move(config)))
{ }

TFuture<TTimestamp> TTransactionManager::GenerateTimestamp()
{
    return Impl_->GenerateTimestamp();
}

TFuture<TTransactionPtr> TTransactionManager::StartReadWriteTransaction()
{
    return Impl_->StartReadWriteTransaction();
}

TFuture<TTransactionPtr> TTransactionManager::StartReadOnlyTransaction(TTimestamp startTimestamp)
{
    return Impl_->StartReadOnlyTransaction(startTimestamp);
}

TTransactionPtr TTransactionManager::FindTransaction(const TTransactionId& id)
{
    return Impl_->FindTransaction(id);
}

TTransactionPtr TTransactionManager::GetTransactionOrThrow(const TTransactionId& id)
{
    return Impl_->GetTransactionOrThrow(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP

