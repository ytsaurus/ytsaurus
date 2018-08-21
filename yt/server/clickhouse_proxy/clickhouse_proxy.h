#include "private.h"

#include <yt/client/api/public.h>

#include <yt/core/http/http.h>

namespace NYT {
namespace NClickHouseProxy {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxy
    : public TRefCounted
{
public:
    TClickHouseProxy(const TClickHouseProxyConfigPtr& config);

    void HandleHttpRequest(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp) const;

    void ReplyWithError(const NHttp::IResponseWriterPtr& rsp, NHttp::EStatusCode statusCode, TError error) const;

    NApi::IClientPtr PrepareClient(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp) const;

private:
    TClickHouseProxyConfigPtr Config_;

    // TODO(max42): implement rpc client cache.
    // TODO(max42): implement caching in discovery.
};

DEFINE_REFCOUNTED_TYPE(TClickHouseProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
