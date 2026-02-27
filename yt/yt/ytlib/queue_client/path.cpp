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

TString ConvertToString(const TGenericObjectPath& path)
{
    TStringBuilder builder;
    AppendAttributes(&builder, path.Attributes());
    builder.AppendString(path.GetPath());
    return builder.Flush();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::weak_ordering operator<=>(const TTablePath& lhs, const TTablePath& rhs)
{
    return std::tuple(lhs.GetCluster(), lhs.GetPath()) <=> std::tuple(rhs.GetCluster(), rhs.GetPath());
}

std::weak_ordering operator<=>(const TGenericObjectPath& lhs, const TGenericObjectPath& rhs)
{
    return std::tuple(lhs.GetCluster(), lhs.GetPath(), lhs.GetQueueConsumerName()) <=> std::tuple(rhs.GetCluster(), rhs.GetPath(), rhs.GetQueueConsumerName());
}

TTablePath ToTablePath(const TGenericObjectPath& genericPath)
{
    TRichYPath path(genericPath.GetPath());
    path.SetCluster(genericPath.GetCluster().value());
    return TTablePath(std::move(path));
}

TCrossClusterReference ToCrossClusterReference(const TTablePath& path)
{
    YT_VERIFY(path.GetCluster().has_value(), "Cluster is required for TTablePath");
    return TCrossClusterReference(path.GetCluster().value(), path.GetPath());
}

TCrossClusterReference ToCrossClusterReference(const TGenericObjectPath& path)
{
    YT_VERIFY(path.GetCluster().has_value(), "Cluster is required for TGenericObjectPath");
    return TCrossClusterReference(path.GetCluster().value(), path.GetPath());
}

void FormatValue(TStringBuilderBase* builder, const TTablePath& path, TStringBuf spec)
{
    // TODO(YT-27209): Remove this implementation.
    FormatValue(builder, ToCrossClusterReference(path), spec);
}

void FormatValue(TStringBuilderBase* builder, const TGenericObjectPath& path, TStringBuf spec)
{
    if (path.GetQueueConsumerName().has_value()) {
        FormatValue(builder, ConvertToString(path), spec);
        return;
    }
    // TODO(YT-27209): Remove this implementation.
    FormatValue(builder, ToCrossClusterReference(path), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueClient

size_t THash<NYT::NQueueClient::TQueuePath>::operator()(
    const NYT::NQueueClient::TQueuePath& path) const
{
    return ComputeHash(ToString(path));
}

size_t THash<NYT::NQueueClient::TConsumerPath>::operator()(
    const NYT::NQueueClient::TConsumerPath& path) const
{
    return ComputeHash(ToString(path));
}
