#pragma once

#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TCreationAttributeMatches
{
    std::vector<TAttributeUpdateMatch> Matches;
    THashSet<const TScalarAttributeSchema*> PendingInitializerAttributes;

    using TMandatoryAttributeKey = std::pair<const TScalarAttributeSchema*, NYPath::TYPath>;
    THashSet<TMandatoryAttributeKey, THash<TMandatoryAttributeKey>, TEqualTo<>> UnmatchedMandatoryAttributes;
};

TCreationAttributeMatches MatchCreationAttributes(
    const IObjectTypeHandler* typeHandler,
    const NYTree::INodePtr& attributes);

////////////////////////////////////////////////////////////////////////////////

struct TKeyAttributeMatches
{
    TObjectKey Key;
    TObjectKey ParentKey;
    TString Uuid;
};

TKeyAttributeMatches MatchKeyAttributes(
    const IObjectTypeHandler* typeHandler,
    const NYTree::INodePtr& attributes,
    TTransaction* transaction = nullptr,
    bool autogenerateKey = false);

void FormatValue(TStringBuilderBase* builder, const TKeyAttributeMatches& match, TStringBuf);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
