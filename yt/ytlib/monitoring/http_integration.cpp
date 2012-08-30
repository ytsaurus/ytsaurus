#include "stdafx.h"
#include "http_integration.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/url.h>
#include <ytlib/formats/json_writer.h>
#include <ytlib/formats/config.h>
#include <ytlib/ytree/ypath_proxy.h>
#include <ytlib/ytree/yson_parser.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/virtual.h>

#include <library/json/json_writer.h>

#include <util/string/vector.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NHttp;

namespace {

Stroka OnResponse(NYTree::TYPathProxy::TRspGetPtr rsp)
{
    if (!rsp->IsOK()) {
        // TODO(sandello): Proper JSON escaping here.
        return FormatInternalServerErrorResponse(ToString(rsp->GetError()).Quote());
    }

    // TODO(babenko): maybe extract method
    TStringStream output;
    NFormats::TJsonWriter writer(&output);
    Consume(TYsonString(rsp->value()), &writer);
    writer.Flush();

    return FormatOKResponse(output.Str());
}

void ParseQuery(IAttributeDictionary* attributes, const Stroka& query)
{
    auto params = splitStroku(query, "&");
    FOREACH (const auto& param, params) {
        auto eqIndex = param.find_first_of('=');
        if (eqIndex == Stroka::npos) {
            THROW_ERROR_EXCEPTION("Missing value of query parameter %s",
                ~param.Quote());
        }
        if (eqIndex == 0) {
            THROW_ERROR_EXCEPTION("Empty query parameter name");
        }

        Stroka key = param.substr(0, eqIndex);
        TYsonString value(param.substr(eqIndex + 1));

        // Just a check, IAttributeDictionary takes raw YSON anyway.
        try {
            TYsonString(value).Validate();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing value of query parameter %s", ~key)
                << ex;
        }

        attributes->SetYson(key, value);
    }
}

// TOOD(babenko): use const&
TFuture<Stroka> HandleRequest(IYPathServicePtr service, Stroka url)
{
    try {
        // TODO(babenko): rewrite using some standard URL parser
        auto unescapedUrl = UnescapeUrl(url);
        auto queryIndex = unescapedUrl.find_first_of('?');
        auto req = TYPathProxy::Get();
        TYPath path;
        if (queryIndex == Stroka::npos) {
            path = unescapedUrl;
        } else {
            path = unescapedUrl.substr(0, queryIndex);
            ParseQuery(&req->Attributes(), unescapedUrl.substr(queryIndex + 1));
        }
        req->SetPath(path);
        return ExecuteVerb(service, req).Apply(BIND(&OnResponse));
    } catch (const std::exception& ex) {
        // TODO(sandello): Proper JSON escaping here.
        return MakeFuture(FormatInternalServerErrorResponse(Stroka(ex.what()).Quote()));
    }
}

} // namespace

TServer::TAsyncHandler GetYPathHttpHandler(IYPathServicePtr service)
{
    return BIND(&HandleRequest, service);
}

TServer::TAsyncHandler GetYPathHttpHandler(TYPathServiceProducer producer)
{
    return GetYPathHttpHandler(IYPathService::FromProducer(producer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
