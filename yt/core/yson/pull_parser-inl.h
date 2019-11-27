#pragma once

#ifndef PULL_PARSER_INL_H_
#error "Direct inclusion of this file is not allowed, include pull_parser.h"
// For the sake of sane code completion.
#include "pull_parser.h"
#endif

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TYsonItem::TYsonItem(const TYsonItem& other)
{
    memcpy(this, &other, sizeof(*this));
}

TYsonItem TYsonItem::Simple(EYsonItemType type)
{
    TYsonItem result;
    result.Type_ = type;
    return result;
}

TYsonItem TYsonItem::Boolean(bool data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::BooleanValue;
    result.Data_.Boolean = data;
    return result;
}

TYsonItem TYsonItem::Int64(i64 data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::Int64Value;
    result.Data_.Int64 = data;
    return result;
}

TYsonItem TYsonItem::Uint64(ui64 data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::Uint64Value;
    result.Data_.Uint64 = data;
    return result;
}

TYsonItem TYsonItem::Double(double data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::DoubleValue;
    result.Data_.Double = data;
    return result;
}

TYsonItem TYsonItem::String(TStringBuf data)
{
    TYsonItem result;
    result.Type_ = EYsonItemType::StringValue;
    result.Data_.String.Ptr = data.data();
    result.Data_.String.Size = data.length();
    return result;
}

EYsonItemType TYsonItem::GetType() const
{
    return Type_;
}

bool TYsonItem::UncheckedAsBoolean() const
{
    YT_ASSERT(GetType() == EYsonItemType::BooleanValue);
    return Data_.Boolean;
}

i64 TYsonItem::UncheckedAsInt64() const
{
    YT_ASSERT(GetType() == EYsonItemType::Int64Value);
    return Data_.Int64;
}

ui64 TYsonItem::UncheckedAsUint64() const
{
    YT_ASSERT(GetType() == EYsonItemType::Uint64Value);
    return Data_.Uint64;
}

double TYsonItem::UncheckedAsDouble() const
{
    YT_ASSERT(GetType() == EYsonItemType::DoubleValue);
    return Data_.Double;
}

TStringBuf TYsonItem::UncheckedAsString() const
{
    YT_ASSERT(GetType() == EYsonItemType::StringValue);
    return TStringBuf(Data_.String.Ptr, Data_.String.Size);
}

bool TYsonItem::IsEndOfStream() const
{
    return GetType() == EYsonItemType::EndOfStream;
}

////////////////////////////////////////////////////////////////////////////////

void NDetail::TZeroCopyInputStreamReader::RefreshBlock()
{
    TotalReadBlocksSize_ += (Current_ - Begin_);
    size_t size = Reader_->Next(&Begin_);
    Current_ = Begin_;
    End_ = Begin_ + size;
    if (size == 0) {
        Finished_ = true;
    }
}

const char* NDetail::TZeroCopyInputStreamReader::Begin() const
{
    return Begin_;
}

const char* NDetail::TZeroCopyInputStreamReader::Current() const
{
    return Current_;
}

const char* NDetail::TZeroCopyInputStreamReader::End() const
{
    return End_;
}

void NDetail::TZeroCopyInputStreamReader::Advance(size_t bytes)
{
    Current_ += bytes;
}

bool NDetail::TZeroCopyInputStreamReader::IsFinished() const
{
    return Finished_;
}

////////////////////////////////////////////////////////////////////////////////

TYsonItem TYsonPullParser::NextImpl()
{
    using namespace NDetail;

    while (true) {
        char ch = Lexer_.GetChar<true>();
        switch (ch) {
            case BeginAttributesSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnAttributesBegin();
                return TYsonItem::Simple(EYsonItemType::BeginAttributes);
            case EndAttributesSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnAttributesEnd();
                return TYsonItem::Simple(EYsonItemType::EndAttributes);
            case BeginMapSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnBeginMap();
                return TYsonItem::Simple(EYsonItemType::BeginMap);
            case EndMapSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnEndMap();
                return TYsonItem::Simple(EYsonItemType::EndMap);
            case BeginListSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnBeginList();
                return TYsonItem::Simple(EYsonItemType::BeginList);
            case EndListSymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnEndList();
                return TYsonItem::Simple(EYsonItemType::EndList);
            case '"': {
                Lexer_.Advance(1);
                TStringBuf value = Lexer_.ReadQuotedString();
                SyntaxChecker_.OnString();
                return TYsonItem::String(value);
            }
            case StringMarker: {
                Lexer_.Advance(1);
                SyntaxChecker_.OnString();
                TStringBuf value = Lexer_.ReadBinaryString();
                return TYsonItem::String(value);
            }
            case Int64Marker: {
                Lexer_.Advance(1);
                i64 value = Lexer_.ReadBinaryInt64();
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Int64Value);
                return TYsonItem::Int64(value);
            }
            case Uint64Marker: {
                Lexer_.Advance(1);
                ui64 value = Lexer_.ReadBinaryUint64();
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Uint64Value);
                return TYsonItem::Uint64(value);
            }
            case DoubleMarker: {
                Lexer_.Advance(1);
                double value = Lexer_.ReadBinaryDouble();
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
                return TYsonItem::Double(value);
            }
            case FalseMarker: {
                Lexer_.Advance(1);
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
                return TYsonItem::Boolean(false);
            }
            case TrueMarker: {
                Lexer_.Advance(1);
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
                return TYsonItem::Boolean(true);
            }
            case EntitySymbol:
                Lexer_.Advance(1);
                SyntaxChecker_.OnSimpleNonstring(EYsonItemType::EntityValue);
                return TYsonItem::Simple(EYsonItemType::EntityValue);
            case EndSymbol:
                SyntaxChecker_.OnFinish();
                return TYsonItem::Simple(EYsonItemType::EndOfStream);
            case '%': {
                Lexer_.Advance(1);
                ch = Lexer_.template GetChar<false>();
                if (ch == 't' || ch == 'f') {
                    SyntaxChecker_.OnSimpleNonstring(EYsonItemType::BooleanValue);
                    return TYsonItem::Boolean(Lexer_.template ReadBoolean<false>());
                } else {
                    SyntaxChecker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
                    return TYsonItem::Double(Lexer_.template ReadNanOrInf<false>());
                }
            }
            case '=':
                SyntaxChecker_.OnEquality();
                Lexer_.Advance(1);
                continue;
            case ';':
                SyntaxChecker_.OnSeparator();
                Lexer_.Advance(1);
                continue;
            default:
                if (isspace(ch)) {
                    Lexer_.SkipSpaceAndGetChar<true>();
                    continue;
                } else if (isdigit(ch) || ch == '-' || ch == '+') { // case of '+' is handled in AfterPlus state
                    TStringBuf valueBuffer;

                    ENumericResult numericResult = Lexer_.template ReadNumeric<true>(&valueBuffer);
                    if (numericResult == ENumericResult::Double) {
                        double value;
                        try {
                            value = FromString<double>(valueBuffer);
                        } catch (const std::exception& ex) {
                            THROW_ERROR_EXCEPTION("Failed to parse double literal %Qv",
                                valueBuffer)
                                << ex;
                        }
                        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::DoubleValue);
                        return TYsonItem::Double(value);
                    } else if (numericResult == ENumericResult::Int64) {
                        i64 value;
                        try {
                            value = FromString<i64>(valueBuffer);
                        } catch (const std::exception& ex) {
                            THROW_ERROR_EXCEPTION("Failed to parse int64 literal %Qv",
                                valueBuffer)
                                << ex;
                        }
                        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Int64Value);
                        return TYsonItem::Int64(value);
                    } else if (numericResult == ENumericResult::Uint64) {
                        ui64 value;
                        try {
                            value = FromString<ui64>(valueBuffer.SubStr(0, valueBuffer.size() - 1));
                        } catch (const std::exception& ex) {
                            THROW_ERROR_EXCEPTION("Failed to parse uint64 literal %Qv",
                                valueBuffer)
                                << ex;
                        }
                        SyntaxChecker_.OnSimpleNonstring(EYsonItemType::Uint64Value);
                        return TYsonItem::Uint64(value);
                    }
                } else if (isalpha(ch) || ch == '_') {
                    TStringBuf value = Lexer_.template ReadUnquotedString<true>();
                    SyntaxChecker_.OnString();
                    return TYsonItem::String(value);
                } else {
                    THROW_ERROR_EXCEPTION("Unexpected %Qv while parsing node", ch);
                }
        }
    }
}

size_t TYsonPullParser::GetNestingLevel() const
{
    return SyntaxChecker_.GetNestingLevel();
}

bool TYsonPullParser::IsOnValueBoundary(size_t nestingLevel) const
{
    return SyntaxChecker_.IsOnValueBoundary(nestingLevel);
}

////////////////////////////////////////////////////////////////////////////////

TYsonPullParserCursor::TYsonPullParserCursor(TYsonItem current, TYsonPullParser* parser)
    : Current_(std::move(current))
    , Parser_(parser)
{ }

TYsonPullParserCursor::TYsonPullParserCursor(TYsonPullParser* parser)
    : TYsonPullParserCursor(parser->Next(), parser)
{ }

const TYsonItem& TYsonPullParserCursor::GetCurrent() const
{
    return Current_;
}

const TYsonItem* TYsonPullParserCursor::operator->() const
{
    return &GetCurrent();
}

const TYsonItem& TYsonPullParserCursor::operator*() const
{
    return GetCurrent();
}

void TYsonPullParserCursor::Next()
{
    Current_ = Parser_->Next();
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename TFunction, EYsonItemType BeginItemType, EYsonItemType EndItemType>
void ParseComposite(TYsonPullParserCursor* cursor, TFunction function)
{
    if constexpr (BeginItemType == EYsonItemType::BeginAttributes) {
        EnsureYsonToken("attributes", *cursor, BeginItemType);
    } else if constexpr (BeginItemType == EYsonItemType::BeginList) {
        EnsureYsonToken("list", *cursor, BeginItemType);
    } else if constexpr (BeginItemType == EYsonItemType::BeginMap) {
        EnsureYsonToken("map", *cursor, BeginItemType);
    }

    cursor->Next();
    while ((*cursor)->GetType() != EndItemType) {
        function(cursor);
    }
    cursor->Next();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <typename TFunction>
void TYsonPullParserCursor::ParseMap(TFunction function)
{
    NDetail::ParseComposite<TFunction, EYsonItemType::BeginMap, EYsonItemType::EndMap>(this, function);
}

template <typename TFunction>
void TYsonPullParserCursor::ParseList(TFunction function)
{
    NDetail::ParseComposite<TFunction, EYsonItemType::BeginList, EYsonItemType::EndList>(this, function);
}

template <typename TFunction>
void TYsonPullParserCursor::ParseAttributes(TFunction function)
{
    NDetail::ParseComposite<TFunction, EYsonItemType::BeginAttributes, EYsonItemType::EndAttributes>(this, function);
}

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void EnsureYsonToken(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    EYsonItemType expected)
{
    if (expected != cursor->GetType()) {
        ThrowUnexpectedYsonTokenException(description, cursor, {expected});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
