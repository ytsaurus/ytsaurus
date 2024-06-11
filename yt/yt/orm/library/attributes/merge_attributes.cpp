#include "merge_attributes.h"

#include "helpers.h"
#include "unwrapping_consumer.h"

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
        if (attributeValue.IsEtc) {
            THROW_ERROR_EXCEPTION_UNLESS(node->GetType() == NYTree::ENodeType::Map, "Etc nodes must be maps");
            auto parentNode = NYTree::FindNodeByYPath(rootNode, attributeValue.Path);
            if (!parentNode) {
                parentNode = NYTree::GetEphemeralNodeFactory()->CreateMap();
                SetNodeByYPath(rootNode, attributeValue.Path, parentNode, /*force*/ true);
            }
            THROW_ERROR_EXCEPTION_UNLESS(parentNode->GetType() == NYTree::ENodeType::Map,
                "Cannot merge etc node into non-map node %v",
                attributeValue.Path);
            auto parentMapNode = parentNode->AsMap();
            auto mapNode = node->AsMap();
            for (const auto& [name, child] : mapNode->GetChildren()) {
                THROW_ERROR_EXCEPTION_IF(parentMapNode->FindChild(name),
                    "Duplicate key %Qv in attribute %v",
                    name,
                    attributeValue.Path);
                mapNode->RemoveChild(child);
                parentMapNode->AddChild(name, child);
            }
        } else if (attributeValue.Path.empty()) {
            rootNode = node->AsMap();
        } else {
            RemoveEntitiesOnPath(rootNode, attributeValue.Path);
            SetNodeByYPath(rootNode, attributeValue.Path, node, /*force*/ true);
        }
    }

    return ConvertToYsonString(rootNode, format);
}

bool HasPrefixes(const std::vector<TAttributeValue>& attributeValues)
{
    for (size_t i = 0; i < attributeValues.size(); i++) {
        for (size_t j = 0; j < attributeValues.size(); j++) {
            if (i != j && NYPath::HasPrefix(attributeValues[i].Path, attributeValues[j].Path)) {
                return true;
            }
        }
    }

    return false;
}

} // anonymous namespace

//////////////////////////////////////////////////////////////////////////////

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

void SortAndRemoveNestedPaths(std::vector<NYPath::TYPath>& paths)
{
    std::sort(paths.begin(), paths.end());

    int lastRemainingPath = 0;
    for (int i = 1; i < std::ssize(paths); ++i) {
        if (!NYPath::HasPrefix(paths[i], paths[lastRemainingPath])) {
            paths[++lastRemainingPath] = paths[i];
        }
    }

    paths.resize(lastRemainingPath + 1);
}

//////////////////////////////////////////////////////////////////////////////

TMergeAttributesHelper::TMergeAttributesHelper(NYson::IYsonConsumer* consumer)
    : Consumer_(consumer)
{
    Consumer_->OnBeginMap();
}

void TMergeAttributesHelper::ToNextPath(NYPath::TYPathBuf path, bool isEtc)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Expect(NYPath::ETokenType::StartOfStream);
    tokenizer.Advance();

    int matchedPrefixLen = 0;
    while (matchedPrefixLen < std::ssize(PathToCurrentMap_) &&
        tokenizer.GetType() != NYPath::ETokenType::EndOfStream)
    {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        if (tokenizer.GetLiteralValue() == PathToCurrentMap_[matchedPrefixLen]) {
            tokenizer.Advance();
            ++matchedPrefixLen;
        } else {
            break;
        }
    }

    while (std::ssize(PathToCurrentMap_) > matchedPrefixLen) {
        Consumer_->OnEndMap();
        PathToCurrentMap_.pop_back();
    }

    if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
        // Current path turned out to be prefix of previous one.
        // That is supported only if current path is etc.
        YT_VERIFY(isEtc);
        return;
    }

    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        tokenizer.Skip(NYPath::ETokenType::Slash);
        tokenizer.Expect(NYPath::ETokenType::Literal);
        auto lastLiteral = tokenizer.GetLiteralValue();
        Consumer_->OnKeyedItem(lastLiteral);

        if (tokenizer.Advance() != NYPath::ETokenType::EndOfStream || isEtc) {
            Consumer_->OnBeginMap();
            PathToCurrentMap_.push_back(lastLiteral);
        }
    }
}

void TMergeAttributesHelper::Finalize()
{
    while (std::ssize(PathToCurrentMap_) > 0) {
        Consumer_->OnEndMap();
        PathToCurrentMap_.pop_back();
    }

    Consumer_->OnEndMap();
}

//////////////////////////////////////////////////////////////////////////////

TYsonString MergeAttributes(
    std::vector<TAttributeValue> attributeValues,
    NYson::EYsonFormat format)
{
    for (const auto& attribute : attributeValues) {
        THROW_ERROR_EXCEPTION_UNLESS(attribute.Value.GetType() == EYsonType::Node,
            "Yson string type can only be %v",
            EYsonType::Node);
    }

    bool hasEtcs = std::any_of(attributeValues.begin(), attributeValues.end(), [] (const TAttributeValue& value) {
        return value.IsEtc;
    });

    bool allPathsEmpty = std::all_of(attributeValues.begin(), attributeValues.end(), [] (const TAttributeValue& value) {
        return value.Path.Empty();
    });

    if (!hasEtcs && allPathsEmpty && !attributeValues.empty()) {
        return attributeValues.back().Value;
    }

    auto expandedAttributeValues = ExpandWildcardValueLists(std::move(attributeValues), format);

    if (hasEtcs || HasPrefixes(expandedAttributeValues)) {
        return MergeAttributeValuesAsNodes(std::move(expandedAttributeValues), format);
    } else {
        return MergeAttributeValuesAsStrings(std::move(expandedAttributeValues), format);
    }
}

//////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
