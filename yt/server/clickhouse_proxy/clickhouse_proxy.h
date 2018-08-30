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
    TClickHouseProxy(
        const TClickHouseProxyConfigPtr& config,
        TBootstrap* bootstrap);

    void HandleHttpRequest(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp) const;

private:
    TClickHouseProxyConfigPtr Config_;
    TBootstrap* const Bootstrap_;
    NHttp::IClientPtr HttpClient_;

    // TODO(max42): implement rpc client cache.
    // TODO(max42): implement caching in discovery.
};

DEFINE_REFCOUNTED_TYPE(TClickHouseProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
