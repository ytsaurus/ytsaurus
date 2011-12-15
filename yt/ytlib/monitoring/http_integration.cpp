#include "stdafx.h"
#include "http_integration.h"

#include "../ytree/json_adapter.h"
#include "../ytree/ypath_rpc.h"
#include "../ytree/yson_reader.h"
#include "../ytree/ypath_detail.h"

#include "stat.h"

namespace NYT {
namespace NMonitoring {
    
////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NHTTP;

namespace {

Stroka OnResponse(TYPathProxy::TRspGet::TPtr response)
{
    if (!response->IsOK()) {
        // TODO(sandello): Proper JSON escaping here.
        return FormatInternalServerErrorResponse(
            response->GetError().ToString().Quote());
    }

    // TODO(sandello): Use Serialize.h
    TStringStream output;
    TJsonAdapter adapter(&output);
    TYsonReader ysonReader(&adapter);
    TStringStream ysonStream;
    ysonStream << response->value();
    ysonReader.Read(&ysonStream);
    adapter.Flush();

    return FormatOKResponse(output.Str());
}

TFuture<Stroka>::TPtr AsyncGet(IYPathService::TPtr pathService, TYPath path)
{
    if (~pathService == NULL) {
        return ToFuture(FormatServiceUnavailableResponse());
    }

    auto request = TYPathProxy::Get();
    request->SetPath(path);
    auto response = ExecuteVerb(~pathService, ~request);

    return response->Apply(FromMethod(&OnResponse));
}

} // namespace <anonymous>

TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
    TYPathServiceAsyncProvider::TPtr asyncProvider)
{
    return FromFunctor([=] (Stroka path) -> TFuture<Stroka>::TPtr
    {
        return asyncProvider
            ->Do()
            ->Apply(FromMethod(&AsyncGet, path));
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
