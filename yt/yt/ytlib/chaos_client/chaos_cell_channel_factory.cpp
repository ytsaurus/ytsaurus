#include "chaos_cell_channel_factory.h"

#include "chaos_cell_directory_synchronizer.h"
#include "private.h"

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChaosClient {

using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TChaosCellChannelProvider
    : public IRoamingChannelProvider
{
public:
    TChaosCellChannelProvider(
        TCellTag cellTag,
        EPeerKind peerKind,
        ICellDirectoryPtr cellDirectory,
        IChaosCellDirectorySynchronizerPtr synchronizer)
        : CellTag_(cellTag)
        , PeerKind_(peerKind)
        , CellDirectory_(std::move(cellDirectory))
        , EndpointDescription_(Format("CellTag:%v", cellTag))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("cell_tag").Value(cellTag)
            .EndMap()))
        , Logger(ChaosClientLogger
            .WithTag("ProviderId: %v, CellTag:%v",
                TGuid::Create(),
                cellTag))
    {
        synchronizer->AddCellTag(cellTag);

        YT_UNUSED_FUTURE(synchronizer->Sync());
    }

    const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        auto channel = CellDirectory_->FindChannelByCellTag(CellTag_, PeerKind_);
        if (!channel) {
            YT_LOG_DEBUG("No chaos cell channel found");
            return MakeFuture<IChannelPtr>(TError(
                NRpc::EErrorCode::TransientFailure,
                "No cell with tag %v is known",
                CellTag_));
        }

        return MakeFuture(std::move(channel));
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(const TString& /*serviceName*/) override
    {
        return GetChannel();
    }

    void Terminate(const TError& /*error*/) override
    { }

private:
    const TCellTag CellTag_;
    const EPeerKind PeerKind_;
    const ICellDirectoryPtr CellDirectory_;
    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
    const NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

class TChaosCellChannelFactory
    : public IChaosCellChannelFactory
{
public:
    TChaosCellChannelFactory(
        ICellDirectoryPtr cellDirectory,
        IChaosCellDirectorySynchronizerPtr synchronizer)
        : CellDirectory_(std::move(cellDirectory))
        , Synchronizer_(std::move(synchronizer))
    { }

    IChannelPtr CreateChannel(TCellId cellId, EPeerKind peerKind) override
    {
        return CreateChannel(CellTagFromId(cellId), peerKind);
    }

    IChannelPtr CreateChannel(TCellTag cellTag, EPeerKind peerKind) override
    {
        auto provider = New<TChaosCellChannelProvider>(
            cellTag,
            peerKind,
            CellDirectory_,
            Synchronizer_);
        return CreateRoamingChannel(std::move(provider));
    }

private:
    const ICellDirectoryPtr CellDirectory_;
    const IChaosCellDirectorySynchronizerPtr Synchronizer_;
};

////////////////////////////////////////////////////////////////////////////////

IChaosCellChannelFactoryPtr CreateChaosCellChannelFactory(
    ICellDirectoryPtr cellDirectory,
    IChaosCellDirectorySynchronizerPtr synchronizer)
{
    return New<TChaosCellChannelFactory>(
        std::move(cellDirectory),
        std::move(synchronizer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
