#include "clickhouse_proxy.h"

#include <yt/core/http/http.h>

namespace NYT {
namespace NClickHouseProxy {

using namespace NHttp;

////////////////////////////////////////////////////////////////////////////////

TClickHouseProxy::TClickHouseProxy(const TClickHouseProxyConfigPtr& config)
    : Config_(config)
{ }

void TClickHouseProxy::HandleHttpRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
