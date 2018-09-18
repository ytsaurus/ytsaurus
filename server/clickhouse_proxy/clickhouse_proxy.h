#include "private.h"

#include <yt/client/api/public.h>

#include <yt/core/http/http.h>

namespace NYT {
namespace NClickHouseProxy {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxyHandler
    : public NHttp::IHttpHandler
{
public:
    TClickHouseProxyHandler(
        const TClickHouseProxyConfigPtr& config,
        TBootstrap* bootstrap);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    TClickHouseProxyConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    NHttp::IClientPtr HttpClient_;

    // TODO(max42): implement rpc client cache.
    // TODO(max42): implement caching in discovery.
};

DEFINE_REFCOUNTED_TYPE(TClickHouseProxyHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
