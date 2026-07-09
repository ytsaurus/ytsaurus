#include "admin_http_handlers.h"

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/server.h>

#include <library/cpp/yt/system/exit.h>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NHttp;

////////////////////////////////////////////////////////////////////////////////

class TDieHandler
    : public IHttpHandler
{
public:
    void HandleRequest(const IRequestPtr& /*req*/, const IResponseWriterPtr& rsp) override
    {
        rsp->SetStatus(EStatusCode::OK);
        WaitFor(rsp->Close()).ThrowOnError();
        AbortProcessSilently(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterAdminHttpHandlers(const NHttp::IServerPtr& server)
{
    server->AddHandler("/admin/die", New<TDieHandler>());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
