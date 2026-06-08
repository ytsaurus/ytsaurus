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

class TNamedConsumerReference
    : public NYPath::TConstrainedRichYPath<
        NYPath::TRequiredAttributesValidator<QueueConsumerNameAttributeKey, ClusterAttributeKey>,
        NYPath::TWhitelistAttributesValidator<QueueConsumerNameAttributeKey, ClusterAttributeKey>>
{
public:
    TNamedConsumerReference() = delete;
    using TConstrainedRichYPath::TConstrainedRichYPath;
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
void FormatValue(TStringBuilderBase* builder, const TTablePath& path, TStringBuf spec);

//! Stable format for TGenericObjectReference.
void FormatValue(TStringBuilderBase* builder, const TGenericObjectReference& ref, TStringBuf spec);

//! Stable format for TNamedConsumerReference.
void FormatValue(TStringBuilderBase* builder, const TNamedConsumerReference& ref, TStringBuf spec);

void Serialize(const TTablePath& path, NYson::IYsonConsumer* consumer);

void Serialize(const TGenericObjectReference& ref, NYson::IYsonConsumer* consumer);

void Serialize(const TNamedConsumerReference& ref, NYson::IYsonConsumer* consumer);

NYTree::IAttributeDictionaryPtr MakeAttributesWithCluster(const std::string& cluster);

NYTree::IAttributeDictionaryPtr MakeConsumerAttributes(const std::string& cluster, const std::optional<std::string>& queueConsumerName);

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
