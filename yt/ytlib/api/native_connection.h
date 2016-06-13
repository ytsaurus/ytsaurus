#pragma once

#include "connection.h"

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct INativeConnection
    : public IConnection
{
    virtual TNativeConnectionConfigPtr GetConfig() = 0;

    virtual const NObjectClient::TCellId& GetPrimaryMasterCellId() const = 0;
    virtual NObjectClient::TCellTag GetPrimaryMasterCellTag() const = 0;
    virtual const NObjectClient::TCellTagList& GetSecondaryMasterCellTags() const = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    // TODO(sandello): Consider joining these two in favor of a partitioned channel.
    virtual NRpc::IChannelFactoryPtr GetLightChannelFactory() = 0;
    virtual NRpc::IChannelFactoryPtr GetHeavyChannelFactory() = 0;

    virtual INativeClientPtr CreateNativeClient(const TClientOptions& options = TClientOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeConnection)

////////////////////////////////////////////////////////////////////////////////

struct TNativeConnectionOptions
{
    bool RetryRequestQueueSizeLimitExceeded = false;
};

//! Native connection talks directly to the cluster via internal
//! (and typically not stable) RPC protocols.
INativeConnectionPtr CreateNativeConnection(
    TNativeConnectionConfigPtr config,
    const TNativeConnectionOptions& options = TNativeConnectionOptions());

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

