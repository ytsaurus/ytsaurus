#include "private.h"

#include <yt/core/http/public.h>

namespace NYT {
namespace NClickHouseProxy {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseProxy
    : public TRefCounted
{
public:
    TClickHouseProxy(const TClickHouseProxyConfigPtr& config);

    void HandleHttpRequest(const NHttp::IRequestPtr& req, const NHttp::IResponseWriterPtr& rsp);

private:
    TClickHouseProxyConfigPtr Config_;

    // TODO(max42): implement rpc client cache.
    // TODO(max42): implement caching in discovery.
};

DEFINE_REFCOUNTED_TYPE(TClickHouseProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NClickHouseProxy
} // namespace NYT
