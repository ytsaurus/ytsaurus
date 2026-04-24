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
    TTablePath() = delete;
    using TConstrainedRichYPath::TConstrainedRichYPath;

    //! NB(panesher): All unexpected attributes are ignored during conversion.
    static TTablePath FromRichYPath(const NYPath::TRichYPath& richYPath);
};

class TGenericObjectReference
    : public NYPath::TConstrainedRichYPath<
        NYPath::TRequiredAttributesValidator<ClusterAttributeKey>,
        NYPath::TWhitelistAttributesValidator<QueueConsumerNameAttributeKey, ClusterAttributeKey>>
{
public:
    TGenericObjectReference() = delete;
    using TConstrainedRichYPath::TConstrainedRichYPath;

    //! NB(panesher): All unexpected attributes are ignored during conversion.
    static TGenericObjectReference FromRichYPath(const NYPath::TRichYPath& richYPath);
};

using TConsumerReference = TGenericObjectReference;

std::weak_ordering operator<=>(const TTablePath& lhs, const TTablePath& rhs);
std::weak_ordering operator<=>(const TGenericObjectReference& lhs, const TGenericObjectReference& rhs);

//! NB(panesher): The queue_consumer_name attribute is ignored during conversion.
TTablePath ToTablePath(const TGenericObjectReference& genericRef);

TCrossClusterReference ToCrossClusterReference(const TTablePath& path);

//! NB(panesher): The queue_consumer_name attribute is ignored during conversion.
TCrossClusterReference ToCrossClusterReference(const TGenericObjectReference& ref);

//! Stable format for TTablePath.
void FormatValue(TStringBuilderBase* builder, const NQueueClient::TTablePath& path, TStringBuf spec);

//! Stable format for TGenericObjectReference.
void FormatValue(TStringBuilderBase* builder, const NQueueClient::TGenericObjectReference& ref, TStringBuf spec);

NYTree::IAttributeDictionaryPtr MakeAttributesWithCluster(const std::string& cluster);

} // namespace NYT::NQueueClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NQueueClient::TTablePath>
{
    size_t operator()(const NYT::NQueueClient::TTablePath& path) const;
};

template <>
struct THash<NYT::NQueueClient::TGenericObjectReference>
{
    size_t operator()(const NYT::NQueueClient::TGenericObjectReference& ref) const;
};
