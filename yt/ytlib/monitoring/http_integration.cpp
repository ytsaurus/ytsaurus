#include "stdafx.h"
#include "http_integration.h"

#include <core/misc/url.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/attribute_helpers.h>

#include <core/yson/parser.h>

#include <core/ytree/ypath_detail.h>
#include <core/ytree/virtual.h>

#include <ytlib/formats/json_writer.h>
#include <ytlib/formats/config.h>

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
    auto writer = NFormats::CreateJsonConsumer(&output);
    Consume(TYsonString(rsp->value()), writer.get());

    return FormatOKResponse(output.Str());
}

void ParseQuery(IAttributeDictionary* attributes, const Stroka& query)
{
    auto params = splitStroku(query, "&");
    for (const auto& param : params) {
        auto eqIndex = param.find_first_of('=');
        if (eqIndex == Stroka::npos) {
            THROW_ERROR_EXCEPTION("Missing value of query parameter %Qv",
                param);
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
            THROW_ERROR_EXCEPTION("Error parsing value of query parameter %v", key)
                << ex;
        }

        attributes->SetYson(key, value);
    }
}

TFuture<Stroka> HandleRequest(IYPathServicePtr service, const Stroka& url)
{
    try {
        // TODO(babenko): rewrite using some standard URL parser
        auto unescapedUrl = UnescapeUrl(url);
        auto queryIndex = unescapedUrl.find_first_of('?');
        auto path = queryIndex == Stroka::npos ? unescapedUrl : unescapedUrl.substr(0, queryIndex);
        auto req = TYPathProxy::Get(path);
        if (queryIndex != Stroka::npos) {
            auto options = CreateEphemeralAttributes();
            ParseQuery(options.get(), unescapedUrl.substr(queryIndex + 1));
            ToProto(req->mutable_options(), *options);
        }
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
