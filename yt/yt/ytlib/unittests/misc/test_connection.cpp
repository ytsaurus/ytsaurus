#include "test_connection.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NApi::NNative {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

TTestConnection::TTestConnection(
    NRpc::IChannelFactoryPtr channelFactory,
    NNodeTrackerClient::TNetworkPreferenceList networkPreferenceList,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    INodeMemoryTrackerPtr nodeMemoryTracker)
    : ChannelFactory_(std::move(channelFactory))
    , NetworkPreferenceList_(std::move(networkPreferenceList))
    , Config_(New<TConnectionStaticConfig>())
    , DynamicConfig_(New<TConnectionDynamicConfig>())
    , Invoker_(std::move(invoker))
    , NodeMemoryTracker_(std::move(nodeMemoryTracker))
    , NodeDirectory_(std::move(nodeDirectory))
    , SchedulerChannel_(ChannelFactory_->CreateChannel("scheduler"))
    , BundleControllerChannel_(ChannelFactory_->CreateChannel("bundle_controller_channel"))
    , MediumDirectory_(New<NChunkClient::TMediumDirectory>())
{ }

const TConnectionStaticConfigPtr& TTestConnection::GetStaticConfig() const
{
    return Config_;
}

TConnectionDynamicConfigPtr TTestConnection::GetConfig() const
{
    return DynamicConfig_;
}

const NNodeTrackerClient::TNetworkPreferenceList& TTestConnection::GetNetworks() const
{
    return NetworkPreferenceList_;
}

NRpc::IChannelPtr TTestConnection::CreateChannelByAddress(const TString& address)
{
    return ChannelFactory_->CreateChannel(address);
}

IInvokerPtr TTestConnection::GetInvoker()
{
    return Invoker_;
}

IClientPtr TTestConnection::CreateNativeClient(const TClientOptions& options)
{
    return New<TClient>(this, options, NodeMemoryTracker_);
}

const NRpc::IChannelFactoryPtr& TTestConnection::GetChannelFactory()
{
    return ChannelFactory_;
}

const NNodeTrackerClient::TNodeDirectoryPtr& TTestConnection::GetNodeDirectory()
{
    return NodeDirectory_;
}

NRpc::IChannelPtr TTestConnection::GetMasterChannelOrThrow(
    EMasterChannelKind kind,
    NObjectClient::TCellTag cellTag)
{
    return ChannelFactory_->CreateChannel(Format("master-%v-tag-%v", kind, cellTag));
}

NRpc::IChannelPtr TTestConnection::GetMasterChannelOrThrow(
    EMasterChannelKind kind,
    NObjectClient::TCellId cellId)
{
    return ChannelFactory_->CreateChannel(Format("master-%v-id-%v", kind, cellId));
}

NRpc::IChannelPtr TTestConnection::GetCypressChannelOrThrow(
    EMasterChannelKind kind,
    NObjectClient::TCellTag cellTag)
{
    return ChannelFactory_->CreateChannel(Format("cypress-%v-%v", kind, cellTag));
}

const NRpc::IChannelPtr& TTestConnection::GetSchedulerChannel()
{
    return SchedulerChannel_;
}

const NRpc::IChannelPtr& TTestConnection::GetBundleControllerChannel()
{
    return BundleControllerChannel_;
}

const NTransactionClient::IClockManagerPtr& TTestConnection::GetClockManager()
{
    return ClockManager_;
}

const NHiveClient::ICellDirectoryPtr& TTestConnection::GetCellDirectory()
{
    return CellDirectory_;
}

const NHiveClient::TCellTrackerPtr& TTestConnection::GetDownedCellTracker()
{
    return DownedCellTracker_;
}

const NChunkClient::TMediumDirectoryPtr& TTestConnection::GetMediumDirectory()
{
    return MediumDirectory_;
}

////////////////////////////////////////////////////////////////////////////////

NApi::NNative::IConnectionPtr CreateConnection(
    NRpc::IChannelFactoryPtr channelFactory,
    NNodeTrackerClient::TNetworkPreferenceList networkPreferenceList,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    INodeMemoryTrackerPtr nodeMemoryTracker)
{
    return New<TTestConnection>(
        std::move(channelFactory),
        std::move(networkPreferenceList),
        std::move(nodeDirectory),
        std::move(invoker),
        std::move(nodeMemoryTracker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
