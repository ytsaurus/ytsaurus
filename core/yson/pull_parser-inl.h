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

void NDetail::TYsonSyntaxChecker::OnSimpleNonstring(EYsonItemType itemType)
{
    OnSimple<false>(itemType);
}

void NDetail::TYsonSyntaxChecker::OnString()
{
    OnSimple<true>(EYsonItemType::StringValue);
}

void NDetail::TYsonSyntaxChecker::OnFinish()
{
    const auto state = StateStack_.back();
    switch (state) {
        case EYsonState::Terminated:
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectSeparator:
        case EYsonState::InsideMapFragmentExpectKey:
        case EYsonState::InsideMapFragmentExpectSeparator:
            return;
        default:
            ThrowUnexpectedToken("finish");
    }
}

void NDetail::TYsonSyntaxChecker::OnEquality()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideMapExpectEquality:
            StateStack_.back() = EYsonState::InsideMapExpectValue;
            break;
        case EYsonState::InsideAttributeMapExpectEquality:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectValue;
            break;
        case EYsonState::InsideMapFragmentExpectEquality:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectValue;
            break;
        default:
            ThrowUnexpectedToken("=");
    }
}

void NDetail::TYsonSyntaxChecker::OnSeparator()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideMapFragmentExpectSeparator:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectKey;
            break;
        case EYsonState::InsideMapExpectSeparator:
            StateStack_.back() = EYsonState::InsideMapExpectKey;
            break;
        case EYsonState::InsideAttributeMapExpectSeparator:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectKey;
            break;
        case EYsonState::InsideListExpectSeparator:
            StateStack_.back() = EYsonState::InsideListExpectValue;
            break;
        case EYsonState::InsideListFragmentExpectSeparator:
            StateStack_.back() = EYsonState::InsideListFragmentExpectValue;
            break;
        default:
            ThrowUnexpectedToken(";");
    }
}

void NDetail::TYsonSyntaxChecker::OnBeginList()
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectValue;
            break;
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideListExpectValue);
            break;
        default:
            ThrowUnexpectedToken("[");
    }
    IncrementNestingLevel();
}

void NDetail::TYsonSyntaxChecker::OnEndList()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectSeparator:
            StateStack_.pop_back();
            break;
        default:
            ThrowUnexpectedToken("]");
    }
    DecrementNestingLevel();
}

void NDetail::TYsonSyntaxChecker::OnBeginMap()
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectKey;
            break;
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectSeparator;
            StateStack_.push_back(EYsonState::InsideMapExpectKey);
            break;
        default:
            ThrowUnexpectedToken("{");
    }
    IncrementNestingLevel();
}

void NDetail::TYsonSyntaxChecker::OnEndMap()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideMapExpectKey:
        case EYsonState::InsideMapExpectSeparator:
            StateStack_.pop_back();
            break;
        default:
            ThrowUnexpectedToken("}");
    }
    DecrementNestingLevel();
}

void NDetail::TYsonSyntaxChecker::OnAttributesBegin()
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
            StateStack_.back() = EYsonState::ExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideListFragmentExpectValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideMapFragmentExpectValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideMapExpectValue:
            StateStack_.back() = EYsonState::InsideMapExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;
        case EYsonState::InsideListExpectValue:
            StateStack_.back() = EYsonState::InsideListExpectAttributelessValue;
            StateStack_.push_back(EYsonState::InsideAttributeMapExpectKey);
            break;

        case EYsonState::InsideListFragmentExpectAttributelessValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
        case EYsonState::ExpectAttributelessValue:
        case EYsonState::InsideMapExpectAttributelessValue:
        case EYsonState::InsideListExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
            THROW_ERROR_EXCEPTION("Value cannot have several attribute maps");
        default:
            ThrowUnexpectedToken("<");
    }
    IncrementNestingLevel();
}

void NDetail::TYsonSyntaxChecker::OnAttributesEnd()
{
    switch (StateStack_.back()) {
        case EYsonState::InsideAttributeMapExpectKey:
        case EYsonState::InsideAttributeMapExpectSeparator:
            StateStack_.pop_back();
            break;
        default:
            ThrowUnexpectedToken(">");
    }
    DecrementNestingLevel();
}

size_t NDetail::TYsonSyntaxChecker::GetNestingLevel() const
{
    return NestingLevel_;
}

bool NDetail::TYsonSyntaxChecker::IsOnValueBoundary(size_t nestingLevel) const
{
    if (NestingLevel_ != nestingLevel) {
        return false;
    }
    switch (StateStack_.back()) {
        case EYsonState::Terminated:
        case EYsonState::ExpectValue:
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectSeparator:
        case EYsonState::InsideMapFragmentExpectEquality:
        case EYsonState::InsideMapFragmentExpectSeparator:
        case EYsonState::InsideMapExpectEquality:
        case EYsonState::InsideMapExpectSeparator:
        case EYsonState::InsideAttributeMapExpectEquality:
        case EYsonState::InsideAttributeMapExpectSeparator:
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectSeparator:
            return true;

        default:
            return false;
    }
}

template <bool isString>
void NDetail::TYsonSyntaxChecker::OnSimple(EYsonItemType itemType)
{
    switch (StateStack_.back()) {
        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
            StateStack_.pop_back();
            break;
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListFragmentExpectSeparator;
            break;
        case EYsonState::InsideListExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideListExpectSeparator;
            break;
        case EYsonState::InsideMapFragmentExpectKey:
            if constexpr (isString) {
                StateStack_.back() = EYsonState::InsideMapFragmentExpectEquality;
            } else {
                THROW_ERROR_EXCEPTION("Cannot parse key of map fragment; expected string got %Qv",
                    itemType);
            }
            break;
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapFragmentExpectSeparator;
            break;
        case EYsonState::InsideMapExpectKey:
            if constexpr (isString) {
                StateStack_.back() = EYsonState::InsideMapExpectEquality;
            } else {
                THROW_ERROR_EXCEPTION("Cannot parse key of map; expected \"string\" got %Qv",
                    itemType);
            }
            break;
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
            StateStack_.back() = EYsonState::InsideMapExpectSeparator;
            break;
        case EYsonState::InsideAttributeMapExpectKey:
            if constexpr (isString) {
                StateStack_.back() = EYsonState::InsideAttributeMapExpectEquality;
            } else {
                THROW_ERROR_EXCEPTION("Cannot parse key of attribute map; expected \"string\" got %Qv",
                    itemType);
            }
            break;
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
            StateStack_.back() = EYsonState::InsideAttributeMapExpectSeparator;
            break;
        default:
            ThrowUnexpectedToken("value");
    }
}

void NDetail::TYsonSyntaxChecker::IncrementNestingLevel()
{
    ++NestingLevel_;
    if (NestingLevel_ >= NestingLevelLimit) {
        THROW_ERROR_EXCEPTION("Depth limit exceeded while parsing YSON")
            << TErrorAttribute("limit", NestingLevelLimit);
    }
}

void NDetail::TYsonSyntaxChecker::DecrementNestingLevel()
{
    YT_ASSERT(NestingLevel_ > 0);
    --NestingLevel_;
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

                    ENumericResult numericResult = Lexer_.template ReadNumeric<false>(&valueBuffer);
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
                    TStringBuf value = Lexer_.template ReadUnquotedString<false>();
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

} // namespace NYT::NYson
