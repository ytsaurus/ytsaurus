#include "merge_attributes.h"

#include <yt/yt/core/yson/string_merger.h>
#include <yt/yt/core/ypath/tokenizer.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NOrm::NAttributes {

using namespace NYson;

//////////////////////////////////////////////////////////////////////////////

namespace {

TYsonString MergeValueLists(std::vector<TAttributeValue> attributeValues, EYsonFormat format)
{
    YT_VERIFY(!attributeValues.empty());
    std::vector<NYTree::IListNodePtr> lists;
    for (const auto& attributeValue : attributeValues) {
        auto node = NYTree::ConvertToNode(attributeValue.Value);
        THROW_ERROR_EXCEPTION_UNLESS(node->GetType() == NYTree::ENodeType::List, "\"*\" can only expand lists");
        lists.push_back(node->AsList());
    }

    int listSize = lists[0]->GetChildCount();
    for (const auto& list : lists) {
        THROW_ERROR_EXCEPTION_IF(list->GetChildCount() != listSize,
            "Cannot expand lists with different sizes on the same prefix");
    }

    auto newList = NYTree::GetEphemeralNodeFactory()->CreateList();
    for (int index = 0; index < listSize; ++index) {
        std::vector<TAttributeValue> valuesAtIndex;
        for (const auto& [attributeIndex, attributeValue] : Enumerate(attributeValues)) {
            auto list = lists[attributeIndex];
            valuesAtIndex.push_back({
                .Path = attributeValue.Path,
                .Value = ConvertToYsonString(list->FindChild(index))
            });
        }
        newList->AddChild(NYTree::ConvertToNode(MergeAttributes(valuesAtIndex, format)));
    }
    return ConvertToYsonString(newList, format);
}

std::optional<std::tuple<TStringBuf, TStringBuf>> SplitByAsterisk(const NYPath::TYPath& path)
{
    NYPath::TTokenizer tokenizer(path);
    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        auto prev = tokenizer.GetType();
        auto prefix = tokenizer.GetPrefix();
        auto next = tokenizer.Advance();

        if (prev == NYPath::ETokenType::Slash && next == NYPath::ETokenType::Asterisk) {
            return std::tuple<TStringBuf, TStringBuf>{prefix, tokenizer.GetSuffix()};
        }
    }
    return std::nullopt;
}

std::vector<TAttributeValue> ExpandWildcardValueLists(std::vector<TAttributeValue> attributeValues, EYsonFormat format)
{
    std::vector<TAttributeValue> result;
    THashMap<NYPath::TYPath, std::vector<TAttributeValue>> attributesToExpand;
    for (const auto& attributeValue : attributeValues) {
        auto split = SplitByAsterisk(attributeValue.Path);
        if (!split.has_value()) {
            result.push_back(std::move(attributeValue));
            continue;
        }
        auto [before, after] = split.value();
        attributesToExpand[before].push_back(TAttributeValue{
            .Path = NYPath::TYPath{after},
            .Value = std::move(attributeValue.Value),
        });
    }

    for (auto& [path, attributesOnPath] : attributesToExpand) {
        result.push_back(TAttributeValue{
            .Path = path,
            .Value = MergeValueLists(std::move(attributesOnPath), format),
        });
    }
    return result;
}

void RemoveEntitiesOnPath(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    NYTree::WalkNodeByYPath(node, path, {
        .MissingAttributeHandler = [] (const TString& /*key*/) {
            return nullptr;
        },
        .MissingChildKeyHandler = [] (const NYTree::IMapNodePtr& /*node*/, const TString& /*key*/) {
            return nullptr;
        },
        .MissingChildIndexHandler = [] (const NYTree::IListNodePtr& /*node*/, int /*index*/) {
            return nullptr;
        },
        .NodeCannotHaveChildrenHandler = [] (const NYTree::INodePtr& node) {
            if (node->GetType() == NYTree::ENodeType::Entity) {
                auto parent = node->GetParent();
                if (parent) {
                    parent->RemoveChild(node);
                }
            }
            return nullptr;
        }
    });
}

TYsonString MergeAttributeValuesAsStrings(
    std::vector<TAttributeValue> attributeValues,
    EYsonFormat format)
{
    std::vector<NYPath::TYPath> paths;
    paths.reserve(attributeValues.size());
    std::vector<TYsonStringBuf> stringBufs;
    stringBufs.reserve(attributeValues.size());

    for (auto& attributeValue : attributeValues) {
        paths.push_back(std::move(attributeValue.Path));
        stringBufs.push_back(attributeValue.Value);
    }

    return NYson::MergeYsonStrings(paths, stringBufs, format);
}

TYsonString MergeAttributeValuesAsNodes(
    std::vector<TAttributeValue> attributeValues,
    EYsonFormat format)
{
    std::stable_sort(attributeValues.begin(), attributeValues.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Path.size() < rhs.Path.size();
    });

    auto rootNode = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (auto& attributeValue : attributeValues) {
        auto node = NYTree::ConvertToNode(attributeValue.Value);
        if (attributeValue.Path.empty()) {
            rootNode = node->AsMap();
        } else {
            RemoveEntitiesOnPath(rootNode, attributeValue.Path);
            SetNodeByYPath(rootNode, attributeValue.Path, node, /*force*/true);
        }
    }

    return ConvertToYsonString(rootNode, format);
}

bool IsOnePrefixOfAnother(const NYPath::TYPath& lhs, const NYPath::TYPath& rhs)
{
    auto [lit, rit] = std::mismatch(lhs.begin(), lhs.end(), rhs.begin(), rhs.end());
    if (lit == lhs.end() && (rit == rhs.end() || *rit == '/')) {
        return true;
    }
    if (rit == rhs.end() && (*lit == '/')) {
        return true;
    }
    return false;
}

bool HasPrefixes(const std::vector<TAttributeValue>& attributeValues)
{
    for (size_t i = 0; i < attributeValues.size(); i++) {
        for (size_t j = i + 1; j < attributeValues.size(); j++) {
            if (IsOnePrefixOfAnother(attributeValues[i].Path, attributeValues[j].Path)) {
                return true;
            }
        }
    }

    return false;
}

} // anonymous namespace

TYsonString MergeAttributes(
    std::vector<TAttributeValue> attributeValues,
    NYson::EYsonFormat format)
{
    for (const auto& attribute : attributeValues) {
        THROW_ERROR_EXCEPTION_UNLESS(attribute.Value.GetType() == EYsonType::Node,
            "Yson string type can only be %v",
            EYsonType::Node);
    }

    auto expandedAttributeValues = ExpandWildcardValueLists(std::move(attributeValues), format);

    if (HasPrefixes(expandedAttributeValues)) {
        return MergeAttributeValuesAsNodes(std::move(expandedAttributeValues), format);
    } else {
        return MergeAttributeValuesAsStrings(std::move(expandedAttributeValues), format);
    }
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
