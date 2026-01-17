#include "chaos_object_channel_factory.h"

#include "chaos_cell_directory_synchronizer.h"
#include "config.h"
#include "private.h"
#include "chaos_residency_cache.h"

#include <yt/yt/ytlib/election/alien_cell_peer_channel_factory.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/chaos_client/helpers.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/public.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/rpc/bus/channel.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NChaosClient {

using namespace NRpc;
using namespace NHiveClient;
using namespace NYTree;
using namespace NHydra;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TChaosObjectChannelProvider
    : public IRoamingChannelProvider
{
public:
    TChaosObjectChannelProvider(
        TChaosObjectId chaosObjectId,
        ICellDirectoryPtr cellDirectory,
        IChaosResidencyCachePtr residencyCache,
        IChaosCellDirectorySynchronizerPtr synchronizer,
        EPeerKind peerKind,
        TChaosObjectChannelConfigPtr config)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , ChaosResidencyCache_(std::move(residencyCache))
        , PeerKind_(peerKind)
        , ChaosObjectId_(chaosObjectId)
        , EndpointDescription_(Format("ChaosObjectId: %v, Type: %v", chaosObjectId, TypeFromId(chaosObjectId)))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("chaos_object_id").Value(chaosObjectId)
            .EndMap()))
        , UnavailableError_(TError(NRpc::EErrorCode::Unavailable, "Chaos object channel is not available")
            << TErrorAttribute("endpoint", EndpointDescription_))
        , Logger(ChaosClientLogger()
            .WithTag("ProviderId: %v, ChaosObjectId: %v, Type: %v",
                TGuid::Create(),
                chaosObjectId,
                TypeFromId(chaosObjectId)))
    {
        YT_UNUSED_FUTURE(synchronizer->Sync());
    }

    const std::string& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        if (auto future = ChannelFuture_.Load(); future && (!future.IsSet() || future.Get().IsOK())) {
            return future;
        }

        return CreateChannel();
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    void Terminate(const TError& /*error*/) override
    { }

private:
    const TChaosObjectChannelConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const IChaosResidencyCachePtr ChaosResidencyCache_;
    const EPeerKind PeerKind_;
    const TChaosObjectId ChaosObjectId_;

    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;

    const TError UnavailableError_;

    const NLogging::TLogger Logger;

    NThreading::TAtomicObject<TFuture<IChannelPtr>> ChannelFuture_;

    IChannelPtr Channel_;
    TCellTag CellTag_ = InvalidCellTag;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    void OnChannelFailed(const IChannelPtr& channel, const TError& error)
    {
        YT_LOG_DEBUG(error, "Chaos object channel failed (IsUnavailable: %v)", IsUnavailableError(error));

        auto cellTag = InvalidCellTag;

        if (auto guard = Guard(Lock_); channel == Channel_) {
            std::swap(cellTag,  CellTag_);
            Channel_.Reset();
        }

        if (cellTag != InvalidCellTag) {
            ChaosResidencyCache_->ForceRefresh(ChaosObjectId_, cellTag);
            ChannelFuture_.Store(TFuture<IChannelPtr>());

            YT_LOG_DEBUG("Invalidated chaos object cell tag from residency cache");
        }
    }

    TFuture<IChannelPtr> CreateChannel()
    {
        YT_LOG_DEBUG("Creating new chaos object channel");

        auto future = ChaosResidencyCache_->GetChaosResidency(ChaosObjectId_)
            .Apply(BIND(&TChaosObjectChannelProvider::OnChaosResidencyFound, MakeStrong(this)));

        ChannelFuture_.Store(future);
        return future;
    }

    TFuture<IChannelPtr> OnChaosResidencyFound(TCellTag cellTag)
    {
        YT_LOG_DEBUG("Found chaos object residency (CellTag: %v)",
            cellTag);

        if (auto channel = CellDirectory_->FindChannelByCellTag(cellTag, PeerKind_)) {
            auto detectingChannel = CreateFailureDetectingChannel(
                std::move(channel),
                Config_->RpcAcknowledgementTimeout,
                BIND(&TChaosObjectChannelProvider::OnChannelFailed, MakeWeak(this)),
                BIND(&TChaosObjectChannelProvider::IsUnavailableError));

            {
                auto guard = Guard(Lock_);
                CellTag_ = cellTag;
                Channel_ = detectingChannel;
            }

            YT_LOG_DEBUG("Created chaos object channel");

            return MakeFuture<IChannelPtr>(detectingChannel);
        }

        YT_LOG_DEBUG(
            "Unable to created chaos object channel due to cell tag absence in cell directory (CellTag: %v)",
            cellTag);

        return MakeFuture<IChannelPtr>(UnavailableError_);
    }

    static bool IsUnavailableError(const TError& error)
    {
        auto code = error.GetCode();

        // COMPAT(savrus)
        if (IsChannelFailureError(error) || error.FindMatching(NYT::EErrorCode::Timeout)) {
            return true;
        }

        return code == NChaosClient::EErrorCode::ReplicationCardMigrated ||
            code == NChaosClient::EErrorCode::ReplicationCardNotKnown ||
            code == NChaosClient::EErrorCode::ChaosCellIsNotEnabled;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChaosObjectChannelFactory
    : public IChaosObjectChannelFactory
{
public:
    TChaosObjectChannelFactory(
        ICellDirectoryPtr cellDirectory,
        IChaosResidencyCachePtr residencyCache,
        IChaosCellDirectorySynchronizerPtr synchronizer,
        TChaosObjectChannelConfigPtr config)
        : Config_(std::move(config))
        , CellDirectory_(std::move(cellDirectory))
        , ChaosResidencyCache_(std::move(residencyCache))
        , Synchronizer_(std::move(synchronizer))
    { }

    NRpc::IChannelPtr CreateChannel(TChaosObjectId chaosObjectId, EPeerKind peerKind) override
    {
        auto provider = New<TChaosObjectChannelProvider>(
            chaosObjectId,
            CellDirectory_,
            ChaosResidencyCache_,
            Synchronizer_,
            peerKind,
            Config_);
        return CreateRoamingChannel(std::move(provider));
    }

private:
    const TChaosObjectChannelConfigPtr Config_;
    const ICellDirectoryPtr CellDirectory_;
    const IChaosResidencyCachePtr ChaosResidencyCache_;
    const IChaosCellDirectorySynchronizerPtr Synchronizer_;
};

////////////////////////////////////////////////////////////////////////////////

IChaosObjectChannelFactoryPtr CreateChaosObjectChannelFactory(
    ICellDirectoryPtr cellDirectory,
    IChaosResidencyCachePtr residencyCache,
    IChaosCellDirectorySynchronizerPtr synchronizer,
    TChaosObjectChannelConfigPtr config)
{
    return New<TChaosObjectChannelFactory>(
        std::move(cellDirectory),
        std::move(residencyCache),
        std::move(synchronizer),
        std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
