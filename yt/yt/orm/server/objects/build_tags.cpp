#include "build_tags.h"

#include "attribute_schema.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TTagSet BuildTags(TTagsTreeNode& node, const TTagSet& parentTags)
{
    TTagSet tags = parentTags;
    tags |= node.MessageTags;
    tags.erase(node.RemoveTags.begin(), node.RemoveTags.end());
    tags |= node.AddTags;

    node.AddTags.clear();
    node.RemoveTags.clear();
    node.MessageTags.clear();
    return tags;
}

void DoBuildTagsRecursively(TTagsTreeNode& node, const TTagSet& parentTags = {})
{
    node.Tags = BuildTags(node, parentTags);
    for (auto& [key, child] : node.Children) {
        DoBuildTagsRecursively(child, node.Tags);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TTagSet& lhs, const TTagSet& rhs)
{
    for (TTag tag : lhs) {
        if (!rhs.contains(tag)) {
            return false;
        }
    }
    for (TTag tag : rhs) {
        if (!lhs.contains(tag)) {
            return false;
        }
    }
    return true;
}

bool DoCollapseTagsRecursively(TTagsTreeNode& node)
{
    bool collapse = true;
    for (auto& [name, child] : node.Children) {
        bool childCollapsed = DoCollapseTagsRecursively(child);
        if (!childCollapsed || node.Tags != child.Tags) {
            collapse = false;
        }
    }
    if (collapse) {
        node.Children.clear();
    }
    return collapse;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TTagsIndexEntry> ConstructTrivialIndex(TTagSet tags)
{
    return {{.Path = NYPath::TYPath{}, .Tags = std::move(tags)}};
}

void DoBuildTagsIndex(
    std::vector<TTagsIndexEntry>& index,
    const TTagsTreeNode& node,
    NYPath::TYPath path,
    ETagsIndexType indexType,
    std::string_view etcName = {})
{
    if (indexType == ETagsIndexType::EtcOnly && (!node.Etc || node.EtcName != etcName)) {
        return;
    }
    if (node.Children.empty()) {
        index.push_back({.Path = std::move(path), .Tags = node.Tags});
        return;
    }
    if (node.Repeated) {
        path.append("/*");
    }
    path.append("/");
    for (const auto& [name, child] : node.Children) {
        auto childPath = path;
        childPath.append(name);
        DoBuildTagsIndex(index, child, std::move(childPath), indexType, etcName);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillNonCompositeSchemaTags(
    TScalarAttributeSchema* schema,
    const TTagsTreeNode& node,
    bool propagatingParentTags,
    ETagsIndexType tagsIndexType,
    std::string_view etcName = {})
{
    if (propagatingParentTags) {
        schema->SetTagsIndex(ConstructTrivialIndex(node.Tags));
    } else {
        schema->SetTagsIndex(BuildTagsIndex(node, tagsIndexType, etcName));
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillAttributeSchemaTags(TAttributeSchema* schema, const TTagsTreeNode& node, bool propagatingParentTags = false)
{
    if (auto* compositeSchema = schema->TryAsComposite()) {
        for (auto& [key, child] : compositeSchema->KeyToChild()) {
            if (propagatingParentTags) {
                FillAttributeSchemaTags(child, node, true);
            } else if (auto it = node.Children.find(key); it != node.Children.end()) {
                FillAttributeSchemaTags(child, it->second);
            } else {
                FillAttributeSchemaTags(child, node, true);
            }
        }
        for (auto& [name, etcChild] : compositeSchema->NameToEtcChild()) {
            FillNonCompositeSchemaTags(etcChild, node, propagatingParentTags, ETagsIndexType::EtcOnly, name);
        }
    } else {
        FillNonCompositeSchemaTags(schema->TryAsScalar(), node, propagatingParentTags, ETagsIndexType::All);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void BuildTagsRecursively(TTagsTreeNode& node)
{
    DoBuildTagsRecursively(node);
}

void CollapseTagsRecursively(TTagsTreeNode& node)
{
    DoCollapseTagsRecursively(node);
}

std::vector<TTagsIndexEntry> BuildTagsIndex(
    const TTagsTreeNode& attributeNode,
    ETagsIndexType indexType,
    std::string_view etcName)
{
    std::vector<TTagsIndexEntry> index;
    if (attributeNode.Children.empty()) {
        index = ConstructTrivialIndex(attributeNode.Tags);
    } else {
        auto prefix = attributeNode.Repeated ? "/*/" : "/";
        for (const auto& [name, child] : attributeNode.Children) {
            DoBuildTagsIndex(index, child, prefix + name, indexType, etcName);
        }
    }
    return index;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

void BuildAttributeSchemaTags(TAttributeSchema* rootSchema, TTagsTreeNode rootTagsNode)
{
    NDetail::BuildTagsRecursively(rootTagsNode);
    NDetail::CollapseTagsRecursively(rootTagsNode);
    NDetail::FillAttributeSchemaTags(rootSchema, rootTagsNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
