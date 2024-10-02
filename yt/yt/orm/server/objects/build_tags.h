#pragma once

#include "public.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct TTagsIndexEntry
{
    NYPath::TYPath Path;
    TTagSet Tags;

    bool operator==(const TTagsIndexEntry& rhs) const = default;
};

struct TTagsTreeNode
{
    THashMap<std::string, TTagsTreeNode> Children;
    TTagSet MessageTags{};
    TTagSet AddTags{};
    TTagSet RemoveTags{};
    std::string EtcName;
    bool Etc{};
    bool Repeated{};
    TTagSet Tags{};

    friend bool operator==(const TTagsTreeNode& lhs, const TTagsTreeNode& rhs) = default;
};

////////////////////////////////////////////////////////////////////////////////

void BuildAttributeSchemaTags(TAttributeSchema* rootSchema, TTagsTreeNode rootTagsNode);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

enum class ETagsIndexType
{
    All,
    EtcOnly,
};

void BuildTagsRecursively(TTagsTreeNode& node);
void CollapseTagsRecursively(TTagsTreeNode& node);
std::vector<TTagsIndexEntry> BuildTagsIndex(
    const TTagsTreeNode& attributeNode,
    ETagsIndexType indexType,
    std::string_view etcName = {});

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
