#include "query_context.h"

#include "private.h"
#include "config.h"
#include "helpers.h"
#include "attributes_helpers.h"
#include "chunk_reader.h"
#include "convert_row.h"
#include "document.h"
#include "subquery.h"
#include "subquery_spec.h"
#include "table_reader.h"
#include "table_schema.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client_cache.h>

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/table_client/config.h>
#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/api/native/transaction.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/client/table_client/name_table.h>
#include <yt/client/table_client/schemaful_reader.h>

#include <yt/client/api/file_reader.h>
#include <yt/client/api/transaction.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/optional.h>
#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/yson/string.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/attributes.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/string/join.h>

#include <stack>
#include <vector>

#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace DB;

////////////////////////////////////////////////////////////////////////////////

TQueryContext::TQueryContext(TBootstrap* bootstrap, TQueryId queryId, const DB::Context& context)
    : Logger(ServerLogger)
    , User(TString(context.getClientInfo().initial_user))
    , QueryId(queryId)
    , QueryKind(static_cast<EQueryKind>(context.getClientInfo().query_kind))
    , Bootstrap(bootstrap)
    , RowBuffer(New<TRowBuffer>())
    , Host_(Bootstrap->GetHost())
{
    Logger.AddTag("QueryId: %v", queryId);
    YT_LOG_INFO("Query context created (User: %v, QueryKind: %v)", User, QueryKind);

    const auto& clientInfo = context.getClientInfo();
    YT_LOG_INFO(
        "Query client info (CurrentUser: %v, CurrentQueryId: %v, CurrentAddress: %v, InitialUser: %v, InitialAddress: %v, "
        "InitialQueryId: %v, Interface: %v, ClientHostname: %v, HttpUserAgent: %v)",
        clientInfo.current_user,
        clientInfo.current_query_id,
        clientInfo.current_address.toString(),
        clientInfo.initial_user,
        clientInfo.initial_address.toString(),
        clientInfo.initial_query_id,
        clientInfo.interface == DB::ClientInfo::Interface::TCP
            ? "TCP"
            : clientInfo.interface == DB::ClientInfo::Interface::HTTP
                ? "HTTP"
                : "(n/a)",
        clientInfo.client_hostname,
        clientInfo.http_user_agent);

    Bootstrap->GetControlInvoker()->Invoke(BIND(
        &TClickHouseHost::AdjustQueryCount,
        Bootstrap->GetHost(),
        User,
        QueryKind,
        +1 /* delta */));
}

TQueryContext::~TQueryContext()
{
    YT_LOG_INFO("Query context destroyed");

    Bootstrap->GetControlInvoker()->Invoke(BIND(
        &TClickHouseHost::AdjustQueryCount,
        Bootstrap->GetHost(),
        User,
        QueryKind,
        -1 /* delta */));
}

const NApi::NNative::IClientPtr& TQueryContext::Client() const
{
    ClientLock_.AcquireReader();
    auto clientPresent = static_cast<bool>(Client_);
    ClientLock_.ReleaseReader();

    if (!clientPresent) {
        ClientLock_.AcquireWriter();
        Client_ = Bootstrap->GetClientCache()->GetClient(User);
        ClientLock_.ReleaseWriter();
    }

    return Client_;
}

////////////////////////////////////////////////////////////////////////////////

void SetupHostContext(TBootstrap* bootstrap, DB::Context& context, TQueryId queryId)
{
    if (!queryId) {
        queryId = TQueryId::Create();
    }

    context.getHostContext() = std::make_shared<TQueryContext>(
        bootstrap,
        queryId,
        context);
}

TQueryContext* GetQueryContext(const DB::Context& context)
{
    auto* hostContext = context.getHostContext().get();
    YT_ASSERT(dynamic_cast<TQueryContext*>(hostContext) != nullptr);
    return static_cast<TQueryContext*>(hostContext);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
