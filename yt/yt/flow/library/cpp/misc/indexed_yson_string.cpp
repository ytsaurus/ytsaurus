#include "indexed_yson_string.h"

#include <yt/yt/core/yson/consumer.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/shared_range.h>
#include <library/cpp/yt/yson_string/format.h>

#include <util/stream/mem.h>
#include <util/stream/str.h>

namespace NYT::NFlow {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

TYsonString ExtractYsonSubpath(TYsonStringBuf yson, const NYPath::TYPath& path)
{
    auto result = NYTree::TryGetAny(yson.AsStringBuf(), path);
    if (!result) {
        THROW_ERROR_EXCEPTION("Yson document has no node at %v", path);
    }
    return TYsonString(std::move(*result));
}

// Reads the value at |offset|, at most ~|budget| bytes of it: returns true and sets |end| past the
// value if it fits within |budget|, false once it overflows. |isMap| tells whether it is a map.
bool ProbeValue(const TSharedRef& buffer, i64 offset, i64 budget, i64* end, bool* isMap)
{
    TMemoryInput input(buffer.Begin() + offset, buffer.Size() - offset);
    TYsonPullParser parser(&input, EYsonType::Node);
    auto item = parser.Next();
    *isMap = item.GetType() == EYsonItemType::BeginMap;
    int depth = 0;
    while (true) {
        switch (item.GetType()) {
            case EYsonItemType::BeginMap:
            case EYsonItemType::BeginList:
            case EYsonItemType::BeginAttributes:
                ++depth;
                break;
            case EYsonItemType::EndMap:
            case EYsonItemType::EndList:
                if (--depth == 0) {
                    *end = offset + parser.GetTotalReadSize();
                    return true;
                }
                break;
            case EYsonItemType::EndAttributes:
                // A value always follows the attributes, so this does not complete the value.
                --depth;
                break;
            default:
                if (depth == 0) {
                    *end = offset + parser.GetTotalReadSize();
                    return true;
                }
                break;
        }
        if (static_cast<i64>(parser.GetTotalReadSize()) > budget) {
            return false;
        }
        item = parser.Next();
    }
}

i64 SkipValue(const TSharedRef& buffer, i64 offset)
{
    TMemoryInput input(buffer.Begin() + offset, buffer.Size() - offset);
    TYsonPullParser parser(&input, EYsonType::Node);
    parser.SkipComplexValue();
    return offset + parser.GetTotalReadSize();
}

std::string ReadKey(const TSharedRef& buffer, i64 offset, i64* keyEnd)
{
    TMemoryInput input(buffer.Begin() + offset, buffer.Size() - offset);
    TYsonPullParser parser(&input, EYsonType::Node);
    auto item = parser.Next();
    *keyEnd = offset + parser.GetTotalReadSize();
    return std::string(item.UncheckedAsString());
}

// Advances past yson whitespace so map iteration works on text (not just binary) yson, where tokens
// may be separated by spaces/newlines. Binary yson has none, so this is a no-op there.
i64 SkipWhitespace(const TSharedRef& buffer, i64 offset)
{
    const auto* data = buffer.Begin();
    i64 size = std::ssize(buffer);
    while (offset < size && (data[offset] == ' ' || data[offset] == '\t' || data[offset] == '\n' || data[offset] == '\r')) {
        ++offset;
    }
    return offset;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TIndexedYsonStringPtr TIndexedYsonString::Build(TYsonString yson, i64 leafSizeThreshold)
{
    // Keep the input buffer alive behind a holder and slice against it; leaves stay zero-copy views
    // of it, so nothing is ever copied. (#TYsonString::ToSharedRef() would copy a string-backed value.)
    auto stringBuf = yson.AsStringBuf();
    auto buffer = TSharedRef(stringBuf.data(), stringBuf.size(), MakeSharedRangeHolder(std::move(yson)));
    i64 valueEnd;
    return BuildValue(buffer, /*valueBegin*/ 0, leafSizeThreshold, &valueEnd);
}

// Builds the node for the value at |valueBegin|, reporting its end in |valueEnd|. A value within the
// threshold becomes one raw slice with no children built; only an over-threshold map is descended
// into, iterating its children by offset so the document is parsed once and no node is allocated for
// a subtree that collapses.
TIndexedYsonStringPtr TIndexedYsonString::BuildValue(
    const TSharedRef& buffer,
    i64 valueBegin,
    i64 leafSizeThreshold,
    i64* valueEnd)
{
    auto node = New<TIndexedYsonString>();

    bool isMap;
    if (ProbeValue(buffer, valueBegin, leafSizeThreshold, valueEnd, &isMap)) {
        node->Leaf_ = TYsonString(buffer.Slice(valueBegin, *valueEnd));
        return node;
    }
    if (!isMap) {
        // An over-threshold non-map (e.g. a huge list) stays raw; find its end and slice it.
        *valueEnd = SkipValue(buffer, valueBegin);
        node->Leaf_ = TYsonString(buffer.Slice(valueBegin, *valueEnd));
        return node;
    }

    node->IsLeaf_ = false;
    i64 pos = SkipWhitespace(buffer, SkipWhitespace(buffer, valueBegin) + 1); // Past the '{'.
    while (buffer.Begin()[pos] != NYson::NDetail::EndMapSymbol) {
        i64 keyEnd;
        auto key = ReadKey(buffer, pos, &keyEnd);
        // Skip whitespace, the '=' separator and more whitespace to reach the value.
        i64 childBegin = SkipWhitespace(buffer, SkipWhitespace(buffer, keyEnd) + 1);
        i64 childEnd;
        node->Children_.emplace(std::move(key), BuildValue(buffer, childBegin, leafSizeThreshold, &childEnd));
        pos = SkipWhitespace(buffer, childEnd);
        if (buffer.Begin()[pos] == NYson::NDetail::ItemSeparatorSymbol) {
            pos = SkipWhitespace(buffer, pos + 1);
        }
    }
    *valueEnd = pos + 1; // Past the '}'.
    return node;
}

void TIndexedYsonString::SerializeTo(IYsonConsumer* consumer) const
{
    if (IsLeaf_) {
        consumer->OnRaw(Leaf_);
        return;
    }
    consumer->OnBeginMap();
    for (const auto& [key, child] : Children_) {
        consumer->OnKeyedItem(key);
        child->SerializeTo(consumer);
    }
    consumer->OnEndMap();
}

TYsonString TIndexedYsonString::GetByPath(const NYPath::TYPath& path) const
{
    const auto* node = this;
    NYPath::TTokenizer tokenizer(path);
    while (true) {
        tokenizer.Advance();
        if (tokenizer.GetType() == NYPath::ETokenType::EndOfStream) {
            break;
        }
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        if (node->IsLeaf_) {
            // The path descends into an unparsed leaf; resolve the rest within it.
            return ExtractYsonSubpath(node->Leaf_, TString("/") + tokenizer.GetToken() + tokenizer.GetSuffix());
        }
        auto it = node->Children_.find(std::string(tokenizer.GetLiteralValue()));
        if (it == node->Children_.end()) {
            THROW_ERROR_EXCEPTION("Yson node has no child with key %Qv", tokenizer.GetLiteralValue());
        }
        node = it->second.Get();
    }
    if (node->IsLeaf_) {
        return node->Leaf_;
    }
    // The path stopped at a parsed subtree; re-serialize it.
    TString result;
    TStringOutput out(result);
    TBufferedBinaryYsonWriter writer(&out);
    node->SerializeTo(&writer);
    writer.Flush();
    return TYsonString(result);
}

TIndexedYsonString::TStats TIndexedYsonString::ComputeStats() const
{
    TStats stats;
    stats.NodeCount = 1;
    if (IsLeaf_) {
        stats.LeafCount = 1;
        stats.LeafBytes = std::ssize(Leaf_.AsStringBuf());
        stats.MaxLeafBytes = stats.LeafBytes;
        return stats;
    }
    for (const auto& [key, child] : Children_) {
        auto childStats = child->ComputeStats();
        stats.NodeCount += childStats.NodeCount;
        stats.LeafCount += childStats.LeafCount;
        stats.LeafBytes += childStats.LeafBytes;
        stats.MaxLeafBytes = std::max(stats.MaxLeafBytes, childStats.MaxLeafBytes);
    }
    return stats;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
