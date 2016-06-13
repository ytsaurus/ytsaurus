#pragma once

#include "client.h"

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct INativeClient
    : public IClient
{
    virtual INativeConnectionPtr GetNativeConnection() = 0;

    virtual NRpc::IChannelPtr GetMasterChannelOrThrow(
        EMasterChannelKind kind,
        NObjectClient::TCellTag cellTag = NObjectClient::PrimaryMasterCellTag) = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NRpc::IChannelFactoryPtr GetNodeChannelFactory() = 0;
    virtual NRpc::IChannelFactoryPtr GetHeavyChannelFactory() = 0;
};

DEFINE_REFCOUNTED_TYPE(INativeClient)

///////////////////////////////////////////////////////////////////////////////

INativeClientPtr CreateNativeClient(
    INativeConnectionPtr connection,
    const TClientOptions& options);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

