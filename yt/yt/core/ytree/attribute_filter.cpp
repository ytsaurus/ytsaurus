#include "attribute_filter.h"

#include "node.h"
#include "convert.h"
#include "fluent.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/yson/pull_parser.h>

#include <yt/yt_proto/yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NYTree {

using namespace NYPath;
using namespace NYson;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TAttributeFilter::TAttributeFilter(std::vector<TString> keys, std::vector<TYPath> paths)
    : Keys(std::move(keys))
    , Paths(std::move(paths))
    , Universal(false)
{ }

TAttributeFilter& TAttributeFilter::operator =(std::vector<TString> keys)
{
    Keys = std::move(keys);
    Paths = {};
    Universal = false;

    return *this;
}

TAttributeFilter::operator bool() const
{
    return !Universal;
}

void TAttributeFilter::ValidateKeysOnly(TStringBuf context) const
{
    if (!Paths.empty()) {
        THROW_ERROR_EXCEPTION("Filtering attributes by path is not implemented for %v", context);
    }
}

bool TAttributeFilter::IsEmpty() const
{
    return !Universal && Keys.empty() && Paths.empty();
}

bool TAttributeFilter::AdmitsKeySlow(TStringBuf key) const
{
    if (!*this) {
        return true;
    }
    return std::find(Keys.begin(), Keys.end(), key) != Keys.end() ||
        std::find(Paths.begin(), Paths.end(), "/" + ToYPathLiteral(key)) != Paths.end();
}

////////////////////////////////////////////////////////////////////////////////

// NB: universal filter is represented as an absent protobuf value.

void ToProto(NProto::TAttributeFilter* protoFilter, const TAttributeFilter& filter)
{
    YT_VERIFY(filter);

    ToProto(protoFilter->mutable_keys(), filter.Keys);
    ToProto(protoFilter->mutable_paths(), filter.Paths);
}

void FromProto(TAttributeFilter* filter, const NProto::TAttributeFilter& protoFilter)
{
    filter->Universal = false;
    FromProto(&filter->Keys, protoFilter.keys());
    FromProto(&filter->Paths, protoFilter.paths());
}

void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer)
{
    if (filter) {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("keys").Value(filter.Keys)
                .Item("paths").Value(filter.Paths)
            .EndMap();
    } else {
        BuildYsonFluently(consumer)
            .Entity();
    }
}

void Deserialize(TAttributeFilter& filter, const INodePtr& node)
{
    switch (node->GetType()) {
        case ENodeType::Map: {
            auto mapNode = node->AsMap();

            filter.Universal = false;
            filter.Keys.clear();
            if (auto keysNode = mapNode->FindChild("keys")) {
                filter.Keys = ConvertTo<std::vector<TString>>(keysNode);
            }

            filter.Paths.clear();
            if (auto pathsNode = mapNode->FindChild("paths")) {
                filter.Paths = ConvertTo<std::vector<TString>>(pathsNode);
            }

            break;
        }
        case ENodeType::List: {
            // Compatibility mode with HTTP clients that specify attribute keys as string lists.
            filter.Universal = false;
            filter.Keys = ConvertTo<std::vector<TString>>(node);
            filter.Paths = {};
            break;
        }
        case ENodeType::Entity: {
            filter.Universal = true;
            filter.Keys = {};
            filter.Paths = {};
            break;
        }
        default:
            THROW_ERROR_EXCEPTION("Unexpected attribute filter type: expected \"map\", \"list\" or \"entity\", got %Qlv", node->GetType());
    }
}

void Deserialize(TAttributeFilter& attributeFilter, TYsonPullParserCursor* cursor)
{
    Deserialize(attributeFilter, ExtractTo<NYTree::INodePtr>(cursor));
}

void FormatValue(
    TStringBuilderBase* builder,
    const TAttributeFilter& attributeFilter,
    TStringBuf /*format*/)
{
    if (attributeFilter) {
        builder->AppendFormat("{Keys: %v, Paths: %v}", attributeFilter.Keys, attributeFilter.Paths);
    } else {
        builder->AppendString("(universal)");
    }
}

TString ToString(const TAttributeFilter& attributeFilter)
{
    return ToStringViaBuilder(attributeFilter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
