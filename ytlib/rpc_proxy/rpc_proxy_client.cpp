#include "rpc_proxy_client.h"
#include "api_service_proxy.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/misc/address.h>
#include <yt/core/misc/small_set.h>

#include <yt/ytlib/api/rowset.h>

#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/row_base.h>
#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

TRpcProxyClient::TRpcProxyClient(
    TRpcProxyConnectionPtr connection,
    NRpc::IChannelPtr channel)
    : Connection_(std::move(connection))
    , Channel_(std::move(channel))
{ }

TRpcProxyClient::~TRpcProxyClient()
{ }

TRpcProxyConnectionPtr TRpcProxyClient::GetRpcProxyConnection()
{
    return Connection_;
}

NRpc::IChannelPtr TRpcProxyClient::GetChannel()
{
    return Channel_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
