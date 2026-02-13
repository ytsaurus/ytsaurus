#include "merge_attributes.h"

#include "helpers.h"
#include "unwrapping_consumer.h"

#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/string_merger.h>
#include <yt/yt/core/yson/yson_builder.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <library/cpp/iterator/functools.h>

#include <library/cpp/containers/bitset/bitset.h>

namespace NYT::NOrm::NAttributes {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

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

struct TListParserContext
{
    explicit TListParserContext(TStringBuf buf)
        : Input(buf)
        , Parser(&Input, EYsonType::Node)
        , Cursor(&Parser)
    {
        THROW_ERROR_EXCEPTION_UNLESS(Cursor->GetType() == EYsonItemType::BeginList, "Wildcard \"*\" can only expand lists");
        Cursor.Next();
    }

    TMemoryInput Input;
    TYsonPullParser Parser;
    TYsonPullParserCursor Cursor;
};

TYsonString NewMergeValueLists(std::vector<TAttributeValue> attributeValues, EYsonFormat format)
{
    YT_VERIFY(!attributeValues.empty());
    std::vector<TListParserContext> parsers;
    parsers.reserve(attributeValues.size());
    for (const auto& attributeValue : attributeValues) {
        parsers.emplace_back(attributeValue.Value.AsStringBuf());
    }

    auto parsingFinished = [&parsers] {
        bool allFinished = true;
        bool anyFinished = false;
        for (const auto& [input, parser, cursor] : parsers) {
            allFinished &= cursor->GetType() == EYsonItemType::EndList;
            anyFinished |= cursor->GetType() == EYsonItemType::EndList;
        }
        THROW_ERROR_EXCEPTION_IF(anyFinished && !allFinished, "Cannot expand lists with different sizes on the same prefix");
        return allFinished;
    };

    TYsonStringBuilder resultBuilder(format);
    resultBuilder->OnBeginList();
    while (!parsingFinished()) {
        std::vector<TAttributeValue> valuesAtIndex;
        valuesAtIndex.reserve(attributeValues.size());
        for (const auto& [attributeIndex, attributeValue] : Enumerate(attributeValues)) {
            auto& cursor = parsers[attributeIndex].Cursor;
            TYsonStringBuilder builder(format);
            cursor.TransferComplexValue(builder.GetConsumer());
            valuesAtIndex.push_back({
                .Path = attributeValue.Path,
                .Value = builder.Flush()
            });
        }
        resultBuilder->OnListItem();
        resultBuilder->OnRaw(MergeAttributes(
            std::move(valuesAtIndex),
            format,
            EDuplicatePolicy::PrioritizeColumn,
            EMergeAttributesMode::New));
    }

    resultBuilder->OnEndList();
    return resultBuilder.Flush();
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

std::vector<TAttributeValue> ExpandWildcardValueLists(
    std::vector<TAttributeValue> attributeValues,
    EYsonFormat format,
    EMergeAttributesMode mergeAttributesMode,
    std::function<void(bool, std::vector<NYPath::TYPath>)> mismatchCallback)
{
    std::vector<TAttributeValue> result;
    THashMap<NYPath::TYPath, std::vector<TAttributeValue>> attributesToExpand;
    for (auto& attributeValue : attributeValues) {
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
        TYsonString value;
        switch (mergeAttributesMode) {
            case EMergeAttributesMode::Old:
                value = MergeValueLists(std::move(attributesOnPath), format);
                break;
            case EMergeAttributesMode::New:
                value = NewMergeValueLists(std::move(attributesOnPath), format);
                break;
            case EMergeAttributesMode::Compare: {
                value = NewMergeValueLists(attributesOnPath, format);
                auto oldValue = MergeValueLists(std::move(attributesOnPath), format);
                YT_VERIFY(NYTree::AreNodesEqual(NYTree::ConvertToNode(oldValue), NYTree::ConvertToNode(value)));
                break;
            }
            case EMergeAttributesMode::CompareCallback: {
                value = MergeValueLists(attributesOnPath, format);
                auto newValue = NewMergeValueLists(attributesOnPath, format);
                if (!NYTree::AreNodesEqual(NYTree::ConvertToNode(value), NYTree::ConvertToNode(newValue))) {
                    std::vector<NYPath::TYPath> mismatchedPaths;
                    mismatchedPaths.reserve(attributesOnPath.size());
                    for (const auto& attributeValue : attributesOnPath) {
                        mismatchedPaths.push_back(attributeValue.Path);
                    }
                    mismatchCallback(/*expandWildcardsMismatched*/ true, std::move(mismatchedPaths));
                }
            }
        }
        result.push_back(TAttributeValue{
            .Path = path,
            .Value = value,
        });
    }
    return result;
}

void RemoveEntitiesOnPath(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    NYTree::WalkNodeByYPath(node, path, {
        .MissingAttributeHandler = [] (const std::string& /*key*/) {
            return nullptr;
        },
        .MissingChildKeyHandler = [] (const NYTree::IMapNodePtr& /*node*/, const std::string& /*key*/) {
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
    EYsonFormat format,
    EDuplicatePolicy duplicatePolicy)
{
    std::ranges::stable_sort(attributeValues, std::less{}, [] (const auto& attributeValue) {
        return attributeValue.Path.size();
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

            if (duplicatePolicy == EDuplicatePolicy::PrioritizeEtc &&
                FindNodeByYPathNoThrow(rootNode, attributeValue.Path))
            {
                continue;
            }

            SetNodeByYPath(rootNode, attributeValue.Path, node, /*force*/ true);
        }
    }

    return ConvertToYsonString(rootNode, format);
}

class TRecursiveAttributeMerger;

class TRecursiveAttributeMergeConsumer
    : public TYsonConsumerBase
{
public:
    TRecursiveAttributeMergeConsumer(NYson::IYsonConsumer* underlying, TRecursiveAttributeMerger& merger)
        : Underlying_(underlying)
        , Forward_(underlying)
        , Merger_(merger)
    { }

    void OnStringScalar(TStringBuf value) override
    {
        Forward_->OnStringScalar(value);
    }

    void OnInt64Scalar(i64 value) override
    {
        Forward_->OnInt64Scalar(value);
    }

    void OnUint64Scalar(ui64 value) override
    {
        Forward_->OnUint64Scalar(value);
    }

    void OnDoubleScalar(double value) override
    {
        Forward_->OnDoubleScalar(value);
    }

    void OnBooleanScalar(bool value) override
    {
        Forward_->OnBooleanScalar(value);
    }

    void OnEntity() override;

    void OnBeginList() override;

    void OnListItem() override
    {
        Forward_->OnListItem();
    }

    void OnEndList() override;

    void OnBeginMap() override;

    void OnKeyedItem(TStringBuf key) override;

    void OnEndMap() override;

    void OnBeginAttributes() override
    {
        THROW_ERROR_EXCEPTION("Value attributes are not supported in attribute merging");
    }

    void OnEndAttributes() override
    {
        THROW_ERROR_EXCEPTION("Value attributes are not supported in attribute merging");
    }

private:
    NYson::IYsonConsumer* const Underlying_;
    NYson::IYsonConsumer* Forward_;
    TRecursiveAttributeMerger& Merger_;
    bool Ignoring_ = false;
    int IgnoringDepth_ = 0;
};

std::vector<TStringBuf> TokenizePath(const NYPath::TYPath &path)
{
    std::vector<TStringBuf> tokenizedPath;
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Expect(NYPath::ETokenType::StartOfStream);
    tokenizer.Advance();

    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        tokenizedPath.push_back(tokenizer.GetToken());
        tokenizer.Advance();
    }

    return tokenizedPath;
}

class TRecursiveAttributeMerger
{
public:
    TRecursiveAttributeMerger(
        NYson::IYsonConsumer* consumer,
        std::vector<TAttributeValue> attributeValues,
        EDuplicatePolicy duplicatePolicy)
        : UnderlyingConsumer_(consumer)
        , Consumer_(consumer, *this)
        , DuplicatePolicy_(duplicatePolicy)
        , AttributeValues_(std::move(attributeValues))
        , ProcessedAttributes_(AttributeValues_.size())
    {
        std::ranges::stable_sort(AttributeValues_, /*comparator*/ {}, /*projection*/ &TAttributeValue::Path);
        AttributePaths_.resize(AttributeValues_.size());
        for (int attributeIndex = 0; attributeIndex < std::ssize(AttributeValues_); ++attributeIndex) {
            AttributePaths_[attributeIndex] = TokenizePath(AttributeValues_[attributeIndex].Path);
        }
    }

    void Run()
    {
        UnderlyingConsumer_->OnBeginMap();
        UsedKeysForDepth_.emplace_back();
        for (int attributeIndex = 0; attributeIndex < std::ssize(AttributeValues_); ++attributeIndex) {
            if (!ProcessedAttributes_.contains(attributeIndex)) {
                ProcessAttribute(attributeIndex);
            }
        }
        while (!CurrentPathSegments_.empty()) {
            Consumer_.OnEndMap();
        }
        UnderlyingConsumer_->OnEndMap();
    }

private:
    friend class TRecursiveAttributeMergeConsumer;

    NYson::IYsonConsumer* const UnderlyingConsumer_;
    TRecursiveAttributeMergeConsumer Consumer_;
    const EDuplicatePolicy DuplicatePolicy_;

    std::vector<TAttributeValue> AttributeValues_;
    TBitSet<int> ProcessedAttributes_;
    std::vector<std::vector<TStringBuf>> AttributePaths_;

    std::vector<std::string> CurrentPathSegments_;
    std::vector<THashSet<std::string>> UsedKeysForDepth_;
    TStringBuf LastKey_;

    int CurrentCommonPrefixLengthForAttribute(int attributeIndex) const
    {
        const auto& attributePath = AttributePaths_[attributeIndex];
        int length = 0;
        for (int i = 0; i < std::min(std::ssize(CurrentPathSegments_), std::ssize(attributePath)); ++i) {
            if (CurrentPathSegments_[i] != attributePath[i]) {
                break;
            }
            ++length;
        }
        return length;
    }

    bool IsCurrentPathPrefixOfAttribute(int attributeIndex, std::optional<TStringBuf> key = std::nullopt) const
    {
        if (!key) {
            return CurrentCommonPrefixLengthForAttribute(attributeIndex) == std::ssize(CurrentPathSegments_);
        }
        auto& attributePath = AttributePaths_[attributeIndex];
        return CurrentCommonPrefixLengthForAttribute(attributeIndex) == std::ssize(CurrentPathSegments_) &&
            attributePath.size() > CurrentPathSegments_.size() &&
            *key == attributePath[CurrentPathSegments_.size()];
    }

    void ProcessAttribute(int attributeIndex)
    {
        auto attributeIndexCodicil = TErrorCodicils::MakeGuard("attribute_index", [attributeIndex] () -> std::string {
            return NYT::ToString(attributeIndex);
        });
        auto attributePathCodicil = TErrorCodicils::MakeGuard("attribute_path", [attributePath = AttributeValues_[attributeIndex].Path] () -> std::string {
            return attributePath;
        });
        const auto& attributePath = AttributePaths_[attributeIndex];
        ProcessedAttributes_.insert(attributeIndex);
        int commonPrefixLength = CurrentCommonPrefixLengthForAttribute(attributeIndex);
        while (std::ssize(CurrentPathSegments_) > commonPrefixLength) {
            Consumer_.OnEndMap();
        }
        while (CurrentPathSegments_.size() + 1 < attributePath.size()) {
            Consumer_.OnKeyedItem(attributePath[CurrentPathSegments_.size()]);
            Consumer_.OnBeginMap();
        }
        // Root or etc append.
        if (attributePath.empty() || CurrentPathSegments_.size() == attributePath.size()) {
            TUnwrappingConsumer unwrappingConsumer(&Consumer_);
            unwrappingConsumer.OnRaw(AttributeValues_[attributeIndex].Value.AsStringBuf(), EYsonType::Node);
            return;
        }
        Consumer_.OnKeyedItem(attributePath.back());
        Consumer_.OnRaw(AttributeValues_[attributeIndex].Value.AsStringBuf(), EYsonType::Node);
    }

    bool LookupOverridingAttributeAndSkipCurrent(TStringBuf currentKey)
    {
        for (int attributeIndex = 0; attributeIndex < std::ssize(AttributeValues_); ++attributeIndex) {
            if (!AttributeValues_[attributeIndex].IsEtc &&
                !ProcessedAttributes_.contains(attributeIndex) &&
                CurrentPathSegments_.size() + 1 == AttributePaths_[attributeIndex].size() &&
                AttributePaths_[attributeIndex].back() == currentKey &&
                std::equal(CurrentPathSegments_.begin(), CurrentPathSegments_.end(), AttributePaths_[attributeIndex].begin()))
            {
                if (DuplicatePolicy_ == EDuplicatePolicy::PrioritizeColumn) {
                    return true;
                }
                ProcessedAttributes_.insert(attributeIndex);
                return false;
            }
        }
        return false;
    }

    bool ShouldReplaceCurrentEntityWithMap() const
    {
        for (int attributeIndex = 0; attributeIndex < std::ssize(AttributeValues_); ++attributeIndex) {
            if (!ProcessedAttributes_.contains(attributeIndex) && IsCurrentPathPrefixOfAttribute(attributeIndex, LastKey_)) {
                return true;
            }
        }
        return false;
    }

    void OnKeyedItem(TStringBuf key)
    {
        auto [iter, inserted] = UsedKeysForDepth_.back().emplace(key);
        if (!inserted) {
            std::string prefix = CurrentPathSegments_.empty() ? "" : "/";
            prefix += JoinToString(CurrentPathSegments_, TDefaultFormatter(), "/");
            THROW_ERROR_EXCEPTION("Duplicate key %Qv", key)
                << TErrorAttribute("key_prefix", prefix);
        }
        LastKey_ = key;
    }

    void OnBeginList()
    {
        UsedKeysForDepth_.emplace_back();
        CurrentPathSegments_.emplace_back(LastKey_);
    }

    void OnEndList()
    {
        UsedKeysForDepth_.pop_back();
        CurrentPathSegments_.pop_back();
    }

    void OnBeginMap()
    {
        UsedKeysForDepth_.emplace_back();
        CurrentPathSegments_.emplace_back(LastKey_);
    }

    void OnEndMap()
    {
        for (int attributeIndex = 0; attributeIndex < std::ssize(AttributeValues_); ++attributeIndex) {
            if (!ProcessedAttributes_.contains(attributeIndex) && IsCurrentPathPrefixOfAttribute(attributeIndex)) {
                ProcessAttribute(attributeIndex);
            }
        }
        UsedKeysForDepth_.pop_back();
        CurrentPathSegments_.pop_back();
    }
};

void TRecursiveAttributeMergeConsumer::OnEntity()
{
    if (Merger_.ShouldReplaceCurrentEntityWithMap()) {
        OnBeginMap();
        OnEndMap();
        return;
    }
    Forward_->OnEntity();
}

void TRecursiveAttributeMergeConsumer::OnBeginMap()
{
    if (Ignoring_) {
        IgnoringDepth_++;
        return;
    }
    Merger_.OnBeginMap();
    Forward_->OnBeginMap();
}

void TRecursiveAttributeMergeConsumer::OnKeyedItem(TStringBuf key)
{
    if (Ignoring_) {
        if (IgnoringDepth_ > 0) {
            return;
        }
        Ignoring_ = false;
        Forward_ = Underlying_;
    }
    if (Merger_.LookupOverridingAttributeAndSkipCurrent(key)) {
        Ignoring_ = true;
        IgnoringDepth_ = 0;
        Forward_ = GetNullYsonConsumer();
        return;
    }
    Merger_.OnKeyedItem(key);
    Forward_->OnKeyedItem(key);
}

void TRecursiveAttributeMergeConsumer::OnEndMap()
{
    if (Ignoring_) {
        if (--IgnoringDepth_ == 0) {
            Ignoring_ = false;
            Forward_ = Underlying_;
        }
        return;
    }
    Merger_.OnEndMap();
    Forward_->OnEndMap();
}

void TRecursiveAttributeMergeConsumer::OnBeginList()
{
    Forward_->OnBeginList();
    Merger_.OnBeginList();
}

void TRecursiveAttributeMergeConsumer::OnEndList()
{
    Forward_->OnEndList();
    Merger_.OnEndList();
}

TYsonString MergeAttributeValuesWithPrefixes(
    std::vector<TAttributeValue> attributeValues,
    EYsonFormat format,
    EDuplicatePolicy duplicatePolicy)
{
    TYsonStringBuilder builder(format);
    TRecursiveAttributeMerger helper(
        builder.GetConsumer(),
        std::move(attributeValues),
        duplicatePolicy);
    helper.Run();
    return builder.Flush();
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

////////////////////////////////////////////////////////////////////////////////

bool IsOnePrefixOfAnother(const NYPath::TYPath& lhs, const NYPath::TYPath& rhs)
{
    auto [lit, rit] = std::ranges::mismatch(lhs, rhs);
    if (lit == lhs.end() && (rit == rhs.end() || *rit == '/')) {
        return true;
    }
    if (rit == rhs.end() && (*lit == '/')) {
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

TYsonString MergeAttributes(
    std::vector<TAttributeValue> attributeValues,
    NYson::EYsonFormat format,
    EDuplicatePolicy duplicatePolicy,
    EMergeAttributesMode mergeAttributesMode,
    std::function<void(bool, std::vector<NYPath::TYPath>)> mismatchCallback)
{
    for (const auto& attribute : attributeValues) {
        THROW_ERROR_EXCEPTION_UNLESS(attribute.Value.GetType() == EYsonType::Node,
            "Yson string type can only be %v",
            EYsonType::Node);
    }

    bool hasEtcs = std::ranges::any_of(attributeValues, std::identity{}, &TAttributeValue::IsEtc);

    bool allPathsEmpty = std::ranges::all_of(attributeValues,
        [] (const auto& attribute) {
            return attribute.Path.empty();
        });

    if (!hasEtcs && allPathsEmpty && !attributeValues.empty()) {
        return attributeValues.back().Value;
    }

    auto expandedAttributeValues = ExpandWildcardValueLists(
        std::move(attributeValues),
        format,
        mergeAttributesMode,
        mismatchCallback);

    bool hasPrefixes = HasPrefixes(expandedAttributeValues);
    if (hasPrefixes || hasEtcs) {
        switch (mergeAttributesMode) {
            case EMergeAttributesMode::Old:
                return MergeAttributeValuesAsNodes(std::move(expandedAttributeValues), format, duplicatePolicy);
            case EMergeAttributesMode::New:
                return MergeAttributeValuesWithPrefixes(std::move(expandedAttributeValues), format, duplicatePolicy);
            case EMergeAttributesMode::Compare: {
                auto oldResult = MergeAttributeValuesAsNodes(expandedAttributeValues, format, duplicatePolicy);
                auto newResult = MergeAttributeValuesWithPrefixes(std::move(expandedAttributeValues), format, duplicatePolicy);
                YT_VERIFY(NYTree::AreNodesEqual(NYTree::ConvertToNode(oldResult), NYTree::ConvertToNode(newResult)));
                return newResult;
            }
            case EMergeAttributesMode::CompareCallback: {
                auto oldResult = MergeAttributeValuesAsNodes(expandedAttributeValues, format, duplicatePolicy);
                auto newResult = MergeAttributeValuesWithPrefixes(std::move(expandedAttributeValues), format, duplicatePolicy);
                if (!NYTree::AreNodesEqual(NYTree::ConvertToNode(oldResult), NYTree::ConvertToNode(newResult))) {
                    std::vector<NYPath::TYPath> mismatchedPaths;
                    mismatchedPaths.reserve(expandedAttributeValues.size());
                    for (const auto& attribute : expandedAttributeValues) {
                        mismatchedPaths.push_back(attribute.Path);
                    }
                    mismatchCallback(/*expandWildcardsMismatched*/ false, std::move(mismatchedPaths));
                }
                return oldResult;
            }
        }
    } else {
        return MergeAttributeValuesAsStrings(std::move(expandedAttributeValues), format);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
