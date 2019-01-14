#include "public.h"

#include <yt/core/http/server.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseHandler
    : public NHttp::IHttpHandler
{
public:
    explicit TClickHouseHandler(TBootstrap* bootstrap);

    virtual void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    TBootstrap* const Bootstrap_;
    const TCoordinatorPtr Coordinator_;
    const TClickHouseConfigPtr Config_;
    const NHttp::IClientPtr HttpClient_;
};

DEFINE_REFCOUNTED_TYPE(TClickHouseHandler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
