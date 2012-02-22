#include "stdafx.h"
#include "http_integration.h"

#include <ytlib/ytree/json_adapter.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/yson_reader.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/virtual.h>

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

    // TODO(babenko): maybe extract method
    TStringStream output;
    TJsonAdapter adapter(&output);
    TStringInput input(response.Value());
    TYsonReader reader(&adapter, &input);
    reader.Read();
    adapter.Flush();

    return FormatOKResponse(output.Str());
}

} // namespace <anonymous>

TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
    IYPathService* service)
{
	// TODO(babenko): use AsStrong
	IYPathServicePtr service_ = service;
    return FromFunctor([=] (Stroka path) -> TFuture<Stroka>::TPtr
        {
			return AsyncYPathGet(~service_, path)->Apply(FromMethod(&OnResponse));
        });
}

TServer::TAsyncHandler::TPtr GetYPathHttpHandler(
	TYPathServiceProducer producer)
{
	return GetYPathHttpHandler(~IYPathService::FromProducer(producer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
