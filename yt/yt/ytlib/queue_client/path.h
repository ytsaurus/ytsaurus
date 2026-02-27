#pragma once

#include <yt/yt/client/ypath/rich.h>
#include <yt/yt/client/ypath/rich_constrained.h>

#include <yt/yt/client/queue_client/common.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/common.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NQueueClient {

inline constexpr char QueueConsumerNameAttributeKey[] = "queue_consumer_name";
inline constexpr char ClusterAttributeKey[] = "cluster";

class TTablePath
    : public NYPath::TConstrainedRichYPath<
        NYPath::TRequiredAttributesValidator<ClusterAttributeKey>,
        NYPath::TWhitelistAttributesValidator<ClusterAttributeKey>>
{
public:
    using TConstrainedRichYPath::TConstrainedRichYPath;
};

using TQueuePath = TTablePath;
using TMultiConsumerPath = TTablePath;

class TGenericObjectPath
    : public NYPath::TConstrainedRichYPath<
        NYPath::TRequiredAttributesValidator<ClusterAttributeKey>,
        NYPath::TWhitelistAttributesValidator<QueueConsumerNameAttributeKey, ClusterAttributeKey>>
{
public:
    using TConstrainedRichYPath::TConstrainedRichYPath;
};

using TConsumerPath = TGenericObjectPath;

std::weak_ordering operator<=>(const TTablePath& lhs, const TTablePath& rhs);
std::weak_ordering operator<=>(const TGenericObjectPath& lhs, const TGenericObjectPath& rhs);

//! NB(panesher): The queue_consumer_name attribute is ignored during conversion.
TTablePath ToTablePath(const TGenericObjectPath& genericPath);

TCrossClusterReference ToCrossClusterReference(const TTablePath& path);

//! NB(panesher): The queue_consumer_name attribute is ignored during conversion.
TCrossClusterReference ToCrossClusterReference(const TGenericObjectPath& path);

//! Stable format for TTablePath.
void FormatValue(TStringBuilderBase* builder, const NQueueClient::TTablePath& path, TStringBuf spec);

//! Stable format for TGenericObjectPath.
void FormatValue(TStringBuilderBase* builder, const NQueueClient::TGenericObjectPath& path, TStringBuf spec);

} // namespace NYT::NQueueClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueueClient::TQueuePath>
{
    size_t operator()(const NYT::NQueueClient::TQueuePath& path) const;
};

template <>
struct THash<NYT::NQueueClient::TConsumerPath>
{
    size_t operator()(const NYT::NQueueClient::TConsumerPath& path) const;
};
