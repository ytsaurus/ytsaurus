#include "chaos_client.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NChaosClient {

using namespace NObjectClient;
using namespace NRpc;
using namespace NThreading;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFuture<TCellTag> TTestChaosResidencyCache::GetChaosResidency(TObjectId /*objectId*/)
{
    return MakeFuture<TCellTag>(TError("Not implemented"));
}

void TTestChaosResidencyCache::ForceRefresh(
    TObjectId /*objectId*/,
    TCellTag /*cellTag*/)
{ }

void TTestChaosResidencyCache::UpdateChaosObjectResidency(
    TObjectId objectId,
    TCellTag cellTag)
{
    auto guard = Guard(Lock_);
    LastUpdatedResidency_ = std::pair(objectId, cellTag);
}

void TTestChaosResidencyCache::RemoveChaosObjectResidency(TObjectId objectId)
{
    auto guard = Guard(Lock_);
    LastRemovedObjectId_ = objectId;
}

void TTestChaosResidencyCache::PingChaosObjectResidency(TObjectId objectId)
{
    auto guard = Guard(Lock_);
    LastPingedObjectId_ = objectId;
}

void TTestChaosResidencyCache::Clear()
{ }

void TTestChaosResidencyCache::Reconfigure(TChaosResidencyCacheConfigPtr /*config*/)
{ }

std::optional<std::pair<TObjectId, TCellTag>> TTestChaosResidencyCache::GetLastUpdatedResidency() const
{
    auto guard = Guard(Lock_);
    return LastUpdatedResidency_;
}

TObjectId TTestChaosResidencyCache::GetLastRemovedObjectId() const
{
    auto guard = Guard(Lock_);
    return LastRemovedObjectId_;
}

TObjectId TTestChaosResidencyCache::GetLastPingedObjectId() const
{
    auto guard = Guard(Lock_);
    return LastPingedObjectId_;
}

////////////////////////////////////////////////////////////////////////////////

TTestChaosConnection::TTestChaosConnection(
    IChannelFactoryPtr channelFactory,
    IChannelPtr chaosChannel,
    IInvokerPtr invoker,
    INodeMemoryTrackerPtr memoryTracker,
    IChaosResidencyCachePtr residencyCache)
    : TTestConnection(
        std::move(channelFactory),
        /*networkPreferenceList*/ {"default"},
        New<NNodeTrackerClient::TNodeDirectory>(),
        /*nodeStatusDirectory*/ nullptr,
        std::move(invoker),
        std::move(memoryTracker))
    , ChaosChannel_(std::move(chaosChannel))
    , ResidencyCache_(std::move(residencyCache))
    , PrimaryMasterCellId_(MakeRandomId(EObjectType::MasterCell, TCellTag(0xf001)))
{ }

TCellId TTestChaosConnection::GetPrimaryMasterCellId() const
{
    return PrimaryMasterCellId_;
}

TCellTag TTestChaosConnection::GetPrimaryMasterCellTag() const
{
    return CellTagFromId(PrimaryMasterCellId_);
}

TCellTagList TTestChaosConnection::GetSecondaryMasterCellTags() const
{
    return {};
}

TCellId TTestChaosConnection::GetMasterCellId(TCellTag /*cellTag*/) const
{
    return PrimaryMasterCellId_;
}

IChannelPtr TTestChaosConnection::GetChaosChannelByObjectIdOrThrow(
    TChaosObjectId /*objectId*/,
    NHydra::EPeerKind /*peerKind*/)
{
    return ChaosChannel_;
}

const IChaosResidencyCachePtr& TTestChaosConnection::GetChaosResidencyCache()
{
    return ResidencyCache_;
}

////////////////////////////////////////////////////////////////////////////////

void TTestChaosLeaseWatcherClientCallbackState::OnUpdated(
    TChaosLeaseId chaosLeaseId,
    TChaosLeasePtr chaosLease,
    TTimestamp timestamp)
{
    {
        auto guard = Guard(Lock_);
        LastUpdate_ = TUpdate{
            .ChaosLeaseId = chaosLeaseId,
            .ChaosLease = std::move(chaosLease),
            .Timestamp = timestamp,
        };
    }

    UpdatedPromise_.TrySet();
}

void TTestChaosLeaseWatcherClientCallbackState::OnDeleted(TChaosLeaseId chaosLeaseId)
{
    {
        auto guard = Guard(Lock_);
        LastDeletedChaosLeaseId_ = chaosLeaseId;
    }

    DeletedPromise_.TrySet();
}

void TTestChaosLeaseWatcherClientCallbackState::OnUnknown(TChaosLeaseId chaosLeaseId)
{
    {
        auto guard = Guard(Lock_);
        LastUnknownChaosLeaseId_ = chaosLeaseId;
    }

    UnknownPromise_.TrySet();
}

void TTestChaosLeaseWatcherClientCallbackState::OnMigrated(TChaosLeaseId chaosLeaseId)
{
    {
        auto guard = Guard(Lock_);
        LastMigratedChaosLeaseId_ = chaosLeaseId;
    }

    MigratedPromise_.TrySet();
}

void TTestChaosLeaseWatcherClientCallbackState::OnNothingChanged(TChaosLeaseId chaosLeaseId)
{
    {
        auto guard = Guard(Lock_);
        LastUnchangedChaosLeaseId_ = chaosLeaseId;
    }

    UnchangedPromise_.TrySet();
}

TFuture<void> TTestChaosLeaseWatcherClientCallbackState::GetUpdatedFuture() const
{
    return UpdatedPromise_.ToFuture();
}

TFuture<void> TTestChaosLeaseWatcherClientCallbackState::GetDeletedFuture() const
{
    return DeletedPromise_.ToFuture();
}

TFuture<void> TTestChaosLeaseWatcherClientCallbackState::GetUnknownFuture() const
{
    return UnknownPromise_.ToFuture();
}

TFuture<void> TTestChaosLeaseWatcherClientCallbackState::GetMigratedFuture() const
{
    return MigratedPromise_.ToFuture();
}

TFuture<void> TTestChaosLeaseWatcherClientCallbackState::GetUnchangedFuture() const
{
    return UnchangedPromise_.ToFuture();
}

std::optional<TTestChaosLeaseWatcherClientCallbackState::TUpdate>
TTestChaosLeaseWatcherClientCallbackState::GetLastUpdate() const
{
    auto guard = Guard(Lock_);
    return LastUpdate_;
}

TChaosLeaseId TTestChaosLeaseWatcherClientCallbackState::GetLastDeletedChaosLeaseId() const
{
    auto guard = Guard(Lock_);
    return LastDeletedChaosLeaseId_;
}

TChaosLeaseId TTestChaosLeaseWatcherClientCallbackState::GetLastUnknownChaosLeaseId() const
{
    auto guard = Guard(Lock_);
    return LastUnknownChaosLeaseId_;
}

TChaosLeaseId TTestChaosLeaseWatcherClientCallbackState::GetLastMigratedChaosLeaseId() const
{
    auto guard = Guard(Lock_);
    return LastMigratedChaosLeaseId_;
}

TChaosLeaseId TTestChaosLeaseWatcherClientCallbackState::GetLastUnchangedChaosLeaseId() const
{
    auto guard = Guard(Lock_);
    return LastUnchangedChaosLeaseId_;
}

namespace {

class TTestChaosLeaseWatcherClientCallbacks
    : public IChaosLeaseWatcherClientCallbacks
{
public:
    explicit TTestChaosLeaseWatcherClientCallbacks(
        TTestChaosLeaseWatcherClientCallbackStatePtr state)
        : State_(std::move(state))
    { }

    void OnChaosLeaseUpdated(
        TChaosLeaseId chaosLeaseId,
        const TChaosLeasePtr& chaosLease,
        TTimestamp timestamp) override
    {
        State_->OnUpdated(chaosLeaseId, chaosLease, timestamp);
    }

    void OnChaosLeaseDeleted(TChaosLeaseId chaosLeaseId) override
    {
        State_->OnDeleted(chaosLeaseId);
    }

    void OnUnknownChaosLease(TChaosLeaseId chaosLeaseId) override
    {
        State_->OnUnknown(chaosLeaseId);
    }

    void OnChaosLeaseMigrated(TChaosLeaseId chaosLeaseId) override
    {
        State_->OnMigrated(chaosLeaseId);
    }

    void OnNothingChanged(TChaosLeaseId chaosLeaseId) override
    {
        State_->OnNothingChanged(chaosLeaseId);
    }

private:
    const TTestChaosLeaseWatcherClientCallbackStatePtr State_;
};

} // namespace

std::unique_ptr<IChaosLeaseWatcherClientCallbacks> CreateTestChaosLeaseWatcherClientCallbacks(
    TTestChaosLeaseWatcherClientCallbackStatePtr state)
{
    return std::make_unique<TTestChaosLeaseWatcherClientCallbacks>(std::move(state));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
