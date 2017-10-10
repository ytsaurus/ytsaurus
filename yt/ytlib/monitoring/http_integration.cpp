#include "http_integration.h"

#include <yt/ytlib/formats/config.h>
#include <yt/ytlib/formats/json_writer.h>

#include <yt/core/misc/url.h>

#include <yt/core/yson/parser.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_detail.h>
#include <yt/core/ytree/ypath_proxy.h>

#include <util/string/vector.h>

namespace NYT {
namespace NMonitoring {

using namespace NYTree;
using namespace NYson;
using namespace NXHttp;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString OnResponse(const TYPathProxy::TErrorOrRspGetPtr& rspOrError)
{
    if (!rspOrError.IsOK()) {
        // TODO(sandello): Proper JSON escaping here.
        return FormatInternalServerErrorResponse(ToString(TError(rspOrError)).Quote());
    }

    const auto& rsp = rspOrError.Value();
    // TODO(babenko): maybe extract method
    TStringStream output;
    try {
        auto writer = NFormats::CreateJsonConsumer(&output);
        Serialize(TYsonString(rsp->value()), writer.get());
        writer->Flush();
    } catch (const std::exception& ex) {
        // TODO(sandello): Proper JSON escaping here.
        return FormatInternalServerErrorResponse(ToString(ex.what()).Quote());
    }

    return FormatOKResponse(output.Str());
}

void ParseQuery(IAttributeDictionary* attributes, const TString& query)
{
    auto params = SplitStroku(query, "&");
    for (const auto& param : params) {
        auto eqIndex = param.find_first_of('=');
        if (eqIndex == TString::npos) {
            THROW_ERROR_EXCEPTION("Missing value of query parameter %Qv",
                param);
        }
        if (eqIndex == 0) {
            THROW_ERROR_EXCEPTION("Empty query parameter name");
        }

        TString key = param.substr(0, eqIndex);
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

TFuture<TString> HandleRequest(IYPathServicePtr service, const TString& url)
{
    try {
        // TODO(babenko): rewrite using some standard URL parser
        auto unescapedUrl = UnescapeUrl(url);
        auto queryIndex = unescapedUrl.find_first_of('?');
        auto path = queryIndex == TString::npos ? unescapedUrl : unescapedUrl.substr(0, queryIndex);
        auto req = TYPathProxy::Get(path);
        if (queryIndex != TString::npos) {
            auto options = CreateEphemeralAttributes();
            ParseQuery(options.get(), unescapedUrl.substr(queryIndex + 1));
            ToProto(req->mutable_options(), *options);
        }
        return ExecuteVerb(service, req).Apply(BIND(&OnResponse));
    } catch (const std::exception& ex) {
        // TODO(sandello): Proper JSON escaping here.
        return MakeFuture(FormatInternalServerErrorResponse(TString(ex.what()).Quote()));
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
