#include "ypath_resolver.h"

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/error.h>

#include <util/stream/mem.h>

namespace NYT::NYTree {

using NYPath::ETokenType;
using NYPath::TTokenizer;

using NYson::EYsonType;
using NYson::EYsonItemType;
using NYson::TYsonPullParser;
using NYson::TYsonPullParserCursor;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExpectedItem,
    (BeginMapOrList)
    (BeginAttribute)
    (Value)
);

using TResult = std::variant<bool, i64, ui64, double, TString>;

bool ParseListUntilIndex(TYsonPullParserCursor* cursor, int targetIndex)
{
    YT_VERIFY((*cursor)->GetType() == EYsonItemType::BeginList);
    cursor->Next();
    int index = 0;
    while ((*cursor)->GetType() != EYsonItemType::EndList) {
        if (index == targetIndex) {
            return true;
        }
        ++index;
        cursor->SkipComplexValue();
    }
    return false;
}

bool ParseMapOrAttributesUntilKey(TYsonPullParserCursor* cursor, TStringBuf key)
{
    auto endType = EYsonItemType::EndMap;
    if ((*cursor)->GetType() != EYsonItemType::BeginMap) {
        YT_VERIFY((*cursor)->GetType() == EYsonItemType::BeginAttributes);
        endType = EYsonItemType::EndAttributes;
    }
    cursor->Next();
    while ((*cursor)->GetType() != endType) {
        YT_VERIFY((*cursor)->GetType() == EYsonItemType::StringValue);
        if ((*cursor)->UncheckedAsString() == key) {
            cursor->Next();
            return true;
        }
        cursor->Next();
        cursor->SkipComplexValue();
    }
    return false;
}

TResult ParseValue(TYsonPullParserCursor* cursor)
{
    switch ((*cursor)->GetType()) {
        case EYsonItemType::BooleanValue:
            return (*cursor)->UncheckedAsBoolean();
        case EYsonItemType::Int64Value:
            return (*cursor)->UncheckedAsInt64();
        case EYsonItemType::Uint64Value:
            return (*cursor)->UncheckedAsUint64();
        case EYsonItemType::DoubleValue:
            return (*cursor)->UncheckedAsDouble();
        case EYsonItemType::StringValue:
            return TString((*cursor)->UncheckedAsString());
        default:
            YT_ABORT();
    }
}

TString ParseAnyValue(TYsonPullParserCursor* cursor)
{
    TStringStream stream;
    {
        NYson::TCheckedInDebugYsonTokenWriter writer(&stream);
        cursor->TransferComplexValue(&writer);
        writer.Flush();
    }
    return std::move(stream.Str());
}

[[noreturn]] void ThrowUnexpectedToken(const TTokenizer& tokenizer)
{
    THROW_ERROR_EXCEPTION(
        "Unexpected YPath token %Qv while parsing %Qv",
        tokenizer.GetToken(),
        tokenizer.GetInput());
}

std::pair<EExpectedItem, std::optional<TString>> NextToken(TTokenizer* tokenizer)
{
    switch (tokenizer->Advance()) {
        case ETokenType::EndOfStream:
            return {EExpectedItem::Value, std::nullopt};

        case ETokenType::Slash: {
            auto type = tokenizer->Advance();
            auto expected = EExpectedItem::BeginMapOrList;
            if (type == ETokenType::At) {
                type = tokenizer->Advance();
                expected = EExpectedItem::BeginAttribute;
            }
            if (type != ETokenType::Literal) {
                ThrowUnexpectedToken(*tokenizer);
            }
            return {expected, tokenizer->GetLiteralValue()};
        }

        default:
            ThrowUnexpectedToken(*tokenizer);
    }
}

std::optional<TResult> TryParseImpl(TStringBuf yson, const TYPath& path, bool isAny)
{
    TTokenizer tokenizer(path);
    TMemoryInput input(yson);
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);

    while (true) {
        auto [expected, literal] = NextToken(&tokenizer);
        if (expected == EExpectedItem::Value && isAny) {
            return {ParseAnyValue(&cursor)};
        }
        if (cursor->GetType() == EYsonItemType::BeginAttributes && expected != EExpectedItem::BeginAttribute) {
            cursor.SkipAttributes();
        }
        switch (cursor->GetType()) {
            case EYsonItemType::BeginAttributes: {
                if (expected != EExpectedItem::BeginAttribute) {
                    return std::nullopt;
                }
                Y_VERIFY(literal.has_value());
                if (!ParseMapOrAttributesUntilKey(&cursor, *literal)) {
                    return std::nullopt;
                }
                break;
            }
            case EYsonItemType::BeginMap: {
                if (expected != EExpectedItem::BeginMapOrList) {
                    return std::nullopt;
                }
                Y_VERIFY(literal.has_value());
                if (!ParseMapOrAttributesUntilKey(&cursor, *literal)) {
                    return std::nullopt;
                }
                break;
            }
            case EYsonItemType::BeginList: {
                if (expected != EExpectedItem::BeginMapOrList) {
                    return std::nullopt;
                }
                Y_VERIFY(literal.has_value());
                int index;
                if (!TryFromString(*literal, index)) {
                    return std::nullopt;
                }
                if (!ParseListUntilIndex(&cursor, index)) {
                    return std::nullopt;
                }
                break;
            }
            case EYsonItemType::EntityValue:
                return std::nullopt;

            case EYsonItemType::BooleanValue:
            case EYsonItemType::Int64Value:
            case EYsonItemType::Uint64Value:
            case EYsonItemType::DoubleValue:
            case EYsonItemType::StringValue:
                if (expected != EExpectedItem::Value) {
                    return std::nullopt;
                }
                return ParseValue(&cursor);
            case EYsonItemType::EndOfStream:
            case EYsonItemType::EndMap:
            case EYsonItemType::EndAttributes:
            case EYsonItemType::EndList:
                YT_ABORT();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::optional<T> TryGetValue(TStringBuf yson, const TYPath& ypath, bool isAny = false)
{
    auto result = NDetail::TryParseImpl(yson, ypath, isAny);
    if (!result.has_value()) {
        return std::nullopt;
    }
    if (const auto* value = std::get_if<T>(&*result)) {
        return *value;
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TryGetInt64(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<i64>(yson, ypath);
}

std::optional<ui64> TryGetUint64(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<ui64>(yson, ypath);
}

std::optional<bool> TryGetBoolean(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<bool>(yson, ypath);
}

std::optional<double> TryGetDouble(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<double>(yson, ypath);
}

std::optional<TString> TryGetString(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<TString>(yson, ypath);
}

std::optional<TString> TryGetAny(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<TString>(yson, ypath, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
