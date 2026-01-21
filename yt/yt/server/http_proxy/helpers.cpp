#include "helpers.h"

#include "coordinator.h"
#include "private.h"

#include <yt/yt/core/http/http.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/proc.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <re2/re2.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;

constinit const auto Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> GatherHeader(const THeadersPtr& headers, const std::string& headerName)
{
    if (auto singleHeader = headers->Find(headerName)) {
        return *singleHeader;
    }

    TString buffer;
    for (int i = 0; ; i++) {
        if (i > 1000) {
            THROW_ERROR_EXCEPTION("Too many header parts")
                << TErrorAttribute("header_name", headerName);
        }

        {
            auto key = Format("%v%v", headerName, i);
            if (auto part = headers->Find(key)) {
                buffer += *part;
                continue;
            }
        }

        {
            auto key = Format("%v-%v", headerName, i);
            if (auto part = headers->Find(key)) {
                buffer += *part;
                continue;
            }
        }

        if (i == 0) {
            return {};
        } else {
            break;
        }
    }

    return Base64Decode(buffer);
}

std::vector<TStringBuf> TokenizeQueryArgumentName(TStringBuf argument)
{
    std::vector<TStringBuf> parts;
    auto openBracket = argument.find('[');
    if (openBracket == TString::npos) {
        openBracket = argument.size();
    }
    parts.push_back(argument.substr(0, openBracket));

    const int MaxNesting = 6;
    int nesting = 0;
    while (openBracket != argument.size()) {
        auto closingBracket = argument.find(']', openBracket);
        if (closingBracket == TString::npos) {
            THROW_ERROR_EXCEPTION("Unmatched bracket in query argument name")
                << TErrorAttribute("argument", argument);
        }

        parts.push_back(argument.substr(openBracket + 1, closingBracket - openBracket - 1));
        openBracket = closingBracket + 1;

        if (++nesting > MaxNesting) {
            THROW_ERROR_EXCEPTION("Nesting limit reached in query argument name")
                << TErrorAttribute("argument", argument);
        }
    }

    return parts;
}

void InsertChildAt(const IMapNodePtr& root, const INodePtr& child, const std::vector<TStringBuf>& at)
{
    auto factory = GetEphemeralNodeFactory();
    INodePtr current = root;
    std::function<void(const INodePtr&)> linkBack;

    const int MaxListSize = 1024;
    for (size_t i = 0; i < at.size(); ++i) {
        i64 intValue = 0;
        if (at[i] == "" || TryFromString(at[i], intValue)) {
            if (!current) {
                current = factory->CreateList();
                linkBack(current);
            }

            auto listNode = current->AsList();
            if (at[i] == "") {
                intValue = listNode->GetChildCount();
            }

            if (intValue < 0 || intValue > MaxListSize) {
                THROW_ERROR_EXCEPTION("Invalid list index in query argument")
                    << TErrorAttribute("index", intValue);
            }

            current = listNode->FindChild(intValue);
            linkBack = [intValue, listNode, factory] (const INodePtr& node) {
                while (intValue >= listNode->GetChildCount()) {
                    listNode->AddChild(factory->CreateEntity());
                }

                listNode->ReplaceChild(listNode->FindChild(intValue), node);
            };
        } else {
            if (!current) {
                current = factory->CreateMap();
                linkBack(current);
            }

            auto mapNode = current->AsMap();
            current = mapNode->FindChild(TString{at[i]});
            linkBack = [key = at[i], mapNode] (const INodePtr& node) {
                mapNode->AddChild(TString{key}, node);
            };
        }
    }

    if (current && (current->GetType() != ENodeType::Entity)) {
        THROW_ERROR_EXCEPTION("Conflicting values in query argument")
            << TErrorAttribute("child", child)
            << TErrorAttribute("conflict", current);
    }

    linkBack(child);
}

IMapNodePtr ParseQueryString(TStringBuf queryString)
{
    auto params = GetEphemeralNodeFactory()->CreateMap();
    TCgiParameters queryParameters(queryString);
    for (auto& param : queryParameters) {
        auto parts = TokenizeQueryArgumentName(param.first);

        i64 intValue;
        if (TryFromString(param.second, intValue)) {
            auto node = GetEphemeralNodeFactory()->CreateInt64();
            node->SetValue(intValue);
            InsertChildAt(params, node, parts);
        } else {
            auto node = GetEphemeralNodeFactory()->CreateString();
            node->SetValue(param.second);
            InsertChildAt(params, node, parts);
        }
    }

    return params;
}

INodePtr DecodeAttributesFromJson(INodePtr node)
{
    if (node->GetType() == ENodeType::Map) {
        auto mapNode = node->AsMap();
        if (mapNode->GetChildCount() > 2) {
            return mapNode;
        }

        auto value = mapNode->FindChild("$value");
        if (!value) {
            return mapNode;
        }
        mapNode->RemoveChild("$value");

        auto attributes = mapNode->FindChild("$attributes");
        if (attributes) {
            mapNode->RemoveChild("$attributes");

            value->MutableAttributes()->MergeFrom(attributes->AsMap());
        }

        return value;
    } else {
        return node;
    }
}

void FixupNodesWithAttributes(const IMapNodePtr& node)
{
    for (auto child : node->GetChildren()) {
        node->RemoveChild(child.second);
        node->AddChild(child.first, DecodeAttributesFromJson(child.second));
    }
}

NYTree::IMapNodePtr HideSecretParameters(const TString& commandName, NYTree::IMapNodePtr parameters)
{
    std::vector<TString> secretParameters = {
        "/spec/secure_vault",
        "/query",
    };

    if (commandName == "poll_job_shell") {
        secretParameters.push_back("/environment");
    }

    if (commandName == "create") {
        secretParameters.push_back("/attributes/value");
    }

    bool needCleanup = false;
    for (const auto& ypath : secretParameters) {
        if (FindNodeByYPath(parameters, ypath)) {
            needCleanup = true;
        }
    }

    if (!needCleanup) {
        return parameters;
    }

    parameters = CloneNode(parameters)->AsMap();
    for (const auto& ypath : secretParameters) {
        if (FindNodeByYPath(parameters, ypath)) {
            SetNodeByYPath(parameters, ypath, ConvertToNode("***"));
        }
    }
    return parameters;
}

std::optional<TPythonWrapperVersion> DetectPythonWrapper(TStringBuf userAgent)
{
    static const re2::RE2 PythonWrapperPattern{"Python wrapper (\\d+).(\\d+).(\\d+)"};
    TPythonWrapperVersion version;
    if (re2::RE2::PartialMatch(
        userAgent.data(),
        PythonWrapperPattern,
        &version.Major,
        &version.Minor,
        &version.Patch))
    {
        return version;
    }

    return {};
}

std::optional<i64> DetectJavaIceberg(TStringBuf userAgent)
{
    static const re2::RE2 JavaIcebergPattern{"iceberg/inside-yt@@(\\d+)"};
    i64 version;
    if (re2::RE2::PartialMatch(
        userAgent.data(),
        JavaIcebergPattern,
        &version))
    {
        return version;
    }

    return {};
}

std::optional<i64> DetectGo(TStringBuf userAgent)
{
    if (userAgent == "go-yt-client") {
        return 0;
    }

    static const re2::RE2 GoPattern{"go-yt-client/(\\d+)"};
    i64 version;
    if (re2::RE2::PartialMatch(
        userAgent.data(),
        GoPattern,
        &version))
    {
        return version;
    }

    return {};
}

bool IsBrowserRequest(const NHttp::IRequestPtr& req)
{
    return req->GetHeaders()->Find("Cookie");
}

bool EnableRequestBodyWorkaround(const NHttp::IRequestPtr& req)
{
    if (IsBrowserRequest(req)) {
        return true;
    }

    auto userAgent = req->GetHeaders()->Find("User-Agent");
    if (!userAgent) {
        return false;
    }

    if (auto version = DetectPythonWrapper(*userAgent); version) {
        if (version->Major == 0 && version->Minor == 8) {
            return version->Patch < 49;
        }

        if (version->Major == 0 && version->Minor == 9) {
            return version->Patch < 3;
        }
    } else if (auto version = DetectJavaIceberg(*userAgent); version) {
        return true;
    }

    return false;
}


////////////////////////////////////////////////////////////////////////////////

std::optional<TNetworkStatistics> GetNetworkStatistics()
{
    try {
        TNetworkStatistics totals;
        for (const auto& iface : GetNetworkInterfaceStatistics()) {
            if (!iface.first.starts_with("eth")) {
                continue;
            }

            totals.TotalRxBytes += iface.second.Rx.Bytes;
            totals.TotalTxBytes += iface.second.Tx.Bytes;
        }

        return totals;
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to read network statistics");
        return {};
    }
}

////////////////////////////////////////////////////////////////////////////////

void ProcessDebugHeaders(const IRequestPtr& /*request*/, const IResponseWriterPtr& response, const TCoordinatorPtr& coordinator)
{
    response->GetHeaders()->Add("X-YT-Proxy", coordinator->GetSelf()->GetHost());
}

void RedirectToDataProxy(const IRequestPtr& request, const IResponseWriterPtr& response, const TCoordinatorPtr& coordinator)
{
    auto target = coordinator->AllocateProxy(coordinator->GetConfig()->DefaultRoleFilter.value_or("data"));
    if (target) {
        auto url = request->GetUrl();
        TString protocol;
        if (request->IsHttps()) {
            protocol = "https";
        } else {
            protocol = "http";
        }

        auto location = Format("%v://%v:%v%v?%v",
            protocol,
            target->GetHost(),
            request->GetPort(),
            url.Path,
            url.RawQuery);

        response->SetStatus(EStatusCode::TemporaryRedirect);
        response->GetHeaders()->Set("Location", location);
        response->AddConnectionCloseHeader();
        WaitFor(response->Close())
            .ThrowOnError();
    } else {
        response->SetStatus(EStatusCode::ServiceUnavailable);
        response->GetHeaders()->Set("Retry-After", "60");
        ReplyError(response, TError("There are no data proxies available"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
