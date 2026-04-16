#include "path.h"

#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/string/string_builder.h>

#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NQueueClient {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NQueueClient;

namespace {

void AppendAttributes(TStringBuilderBase* builder, const IAttributeDictionary& attributes)
{
    auto attributePairs = attributes.ListPairs();
    if (attributePairs.empty()) {
        return;
    }

    TString attrString;
    TStringOutput output(attrString);
    TYsonWriter attrWriter(&output, EYsonFormat::Text, EYsonType::MapFragment);

    std::ranges::sort(attributePairs, [](const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    attrWriter.OnBeginAttributes();
    for (const auto& [key, value] : attributePairs) {
        attrWriter.OnKeyedItem(key);
        attrWriter.OnRaw(value);
    }
    attrWriter.OnEndAttributes();

    builder->AppendString(attrString);
}

TString ConvertToString(const TGenericObjectReference& ref)
{
    TStringBuilder builder;
    AppendAttributes(&builder, ref.Attributes());
    builder.AppendString(ref.GetPath());
    return builder.Flush();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTablePath TTablePath::FromRichYPathSafe(const NYPath::TRichYPath& richYPath)
{
    THROW_ERROR_EXCEPTION_IF(!richYPath.GetCluster().has_value(), "Can't convert %Qv to TTablePath, because there is no cluster attribute", richYPath);
    return TTablePath(richYPath.GetPath(), *MakeAttributesWithCluster(richYPath.GetCluster().value()));
}

TGenericObjectReference TGenericObjectReference::FromRichYPathSafe(const NYPath::TRichYPath& richYPath)
{
    THROW_ERROR_EXCEPTION_IF(!richYPath.GetCluster().has_value(), "Can't convert %Qv to TTablePath, because there is no cluster attribute", richYPath);
    TGenericObjectReference result(richYPath.GetPath(), *MakeAttributesWithCluster(richYPath.GetCluster().value()));
    if (auto consumerName = richYPath.GetQueueConsumerName()) {
        result.SetQueueConsumerName(consumerName.value());
    }
    return result;
}

std::weak_ordering operator<=>(const TTablePath& lhs, const TTablePath& rhs)
{
    return std::tuple(lhs.GetCluster(), lhs.GetPath()) <=> std::tuple(rhs.GetCluster(), rhs.GetPath());
}

std::weak_ordering operator<=>(const TGenericObjectReference& lhs, const TGenericObjectReference& rhs)
{
    return std::tuple(lhs.GetCluster(), lhs.GetPath(), lhs.GetQueueConsumerName()) <=> std::tuple(rhs.GetCluster(), rhs.GetPath(), rhs.GetQueueConsumerName());
}

TTablePath ToTablePath(const TGenericObjectReference& genericRef)
{
    return TTablePath(genericRef.GetPath(), *MakeAttributesWithCluster(genericRef.GetCluster().value()));
}

TCrossClusterReference ToCrossClusterReference(const TTablePath& path)
{
    return TCrossClusterReference(path.GetCluster().value(), path.GetPath());
}

TCrossClusterReference ToCrossClusterReference(const TGenericObjectReference& ref)
{
    return TCrossClusterReference(ref.GetCluster().value(), ref.GetPath());
}

void FormatValue(TStringBuilderBase* builder, const TTablePath& path, TStringBuf spec)
{
    // TODO(YT-27209): Remove this implementation.
    FormatValue(builder, ToCrossClusterReference(path), spec);
}

void FormatValue(TStringBuilderBase* builder, const TGenericObjectReference& ref, TStringBuf spec)
{
    if (ref.GetQueueConsumerName().has_value()) {
        FormatValue(builder, ConvertToString(ref), spec);
        return;
    }
    // TODO(YT-27209): Remove this implementation.
    FormatValue(builder, ToCrossClusterReference(ref), spec);
}

IAttributeDictionaryPtr MakeAttributesWithCluster(const std::string& cluster)
{
    auto attributes = CreateEphemeralAttributes();
    attributes->Set(ClusterAttributeKey, cluster);
    return attributes;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

size_t THash<NYT::NQueueClient::TTablePath>::operator()(
    const NYT::NQueueClient::TTablePath& path) const
{
    return ComputeHash(ToString(path));
}

size_t THash<NYT::NQueueClient::TGenericObjectReference>::operator()(
    const NYT::NQueueClient::TGenericObjectReference& path) const
{
    return ComputeHash(ToString(path));
}
