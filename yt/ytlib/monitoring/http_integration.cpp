#include "stdafx.h"
#include "http_integration.h"

#include "../ytree/json_adapter.h"
#include "../ytree/ypath_proxy.h"
#include "../ytree/yson_reader.h"
#include "../ytree/ypath_detail.h"

#include "stat.h"

namespace NYT {
namespace NMonitoring {
    
////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NHttp;

namespace {

Stroka OnResponse(TValueOrError<TYson> response)
{
    if (!response.IsOK()) {
        // TODO(sandello): Proper JSON escaping here.
        return FormatInternalServerErrorResponse(response.ToString().Quote());
    }

    // TODO(sandello): Use Serialize.h
    TStringStream output;
    TJsonAdapter adapter(&output);
    TStringInput input(response.Value());
    TYsonReader reader(&adapter, &input);
    reader.Read();
    adapter.Flush();

    return FormatOKResponse(output.Str());
}

TFuture<Stroka>::TPtr HandleRequest(TYPathServiceProvider::TPtr provider, const TYPath& path)
{
    auto service = provider->Do();
    if (!service) {
        return ToFuture(FormatServiceUnavailableResponse());
    }
    return AsyncYPathGet(~service, path)->Apply(FromMethod(&OnResponse));
}

} // namespace <anonymous>

TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
    TYPathServiceProvider* provider,
    IInvoker* invoker)
{
    TYPathServiceProvider::TPtr provider_ = provider;
    IInvoker::TPtr invoker_ = invoker;
    return FromFunctor([=] (Stroka path) -> TFuture<Stroka>::TPtr
        {
            return
                FromMethod(
                    &HandleRequest,
                    provider_,
                    path)
                ->AsyncVia(invoker_)
                ->Do();
        });
}

TServer::TSyncHandler::TPtr GetProfilingHttpHandler()
{
    return FromFunctor([] (Stroka path) -> Stroka
        {
            if (path == "/") {
                return FormatOKResponse(
                    NSTAT::GetDump(NSTAT::PLAINTEXT_LATEST));
            } else if (path == "/full") {
                return FormatOKResponse(
                    NSTAT::GetDump(NSTAT::PLAINTEXT_FULL));
            } else if (path == "/fullw") {
                return FormatOKResponse(
                    NSTAT::GetDump(NSTAT::PLAINTEXT_FULL_WITH_TIMES));
            } else {
                return FormatNotFoundResponse();
            }        
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
