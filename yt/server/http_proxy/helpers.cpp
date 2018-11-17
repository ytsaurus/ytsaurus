#include "helpers.h"

#include "private.h"

#include <yt/core/http/http.h>

#include <yt/core/ytree/ephemeral_node_factory.h>

#include <yt/core/misc/proc.h>

#include <library/string_utils/base64/base64.h>

#include <util/string/cgiparam.h>

#include <re2/re2.h>

namespace NYT {
namespace NHttpProxy {

using namespace NHttp;
using namespace NYTree;

static auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TNullable<TString> GatherHeader(const THeadersPtr& headers, const TString& headerName)
{
    auto singleHeader = headers->Find(headerName);
    if (singleHeader) {
        return *singleHeader;
    }

    TString buffer;
    for (int i = 0; ; i++) {
        if (i > 1000) {
            THROW_ERROR_EXCEPTION("Too many header parts")
                << TErrorAttribute("header_name", headerName);
        }

        auto key = headerName + ToString(i);
        auto part = headers->Find(key);
        if (part) {
            buffer += *part;
            continue;
        }

        key = headerName + "-" + ToString(i);
        part = headers->Find(key);
        if (part) {
            buffer += *part;
            continue;
        }

        if (i == 0) {
            return {};
        } else {
            break;
        }
    }

    buffer = Base64Decode(buffer);
    return buffer;
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

    const int MaxListSize = 128;
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

static const re2::RE2 PythonWrapperPattern{"Python wrapper (\\d+).(\\d+).(\\d+)"};

TNullable<TPythonWrapperVersion> DetectPythonWrapper(const TString& userAgent)
{
    TPythonWrapperVersion version;
    if (re2::RE2::PartialMatch(
        userAgent.c_str(),
        PythonWrapperPattern,
        &version.Major,
        &version.Minor,
        &version.Patch))
    {
        return version;
    }

    return {};
}

bool IsWrapperBuggy(const NHttp::IRequestPtr& req)
{
    auto userAgent = req->GetHeaders()->Find("User-Agent");
    if (!userAgent) {
        return false;
    }

    auto version = DetectPythonWrapper(*userAgent);
    if (!version) {
        return false;
    }

    if (version->Major == 0 && version->Minor == 8) {
        return version->Patch < 49;
    }

    if (version->Major == 0 && version->Minor == 9) {
        return version->Patch < 3;
    }

    return false;
}


////////////////////////////////////////////////////////////////////////////////

TNullable<TNetworkStatistics> GetNetworkStatistics()
{
    try {
        TNetworkStatistics totals;
        for (const auto& iface : GetNetworkInterfaceStatistics()) {
            if (!iface.first.StartsWith("eth")) {
                continue;
            }

            totals.TotalRxBytes += iface.second.Rx.Bytes;
            totals.TotalTxBytes += iface.second.Tx.Bytes;
        }

        return totals;
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Failed to read network statistics");
        return {};
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT

