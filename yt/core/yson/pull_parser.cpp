#include "pull_parser.h"

namespace NYT::NYson {

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TZeroCopyInputStreamReader::TZeroCopyInputStreamReader(IZeroCopyInput* reader)
    : Reader_(reader)
{ }

ui64 TZeroCopyInputStreamReader::GetTotalReadSize() const
{
    return TotalReadBlocksSize_ + (Current_ - Begin_);
}

////////////////////////////////////////////////////////////////////////////////

TYsonSyntaxChecker::TYsonSyntaxChecker(EYsonType ysonType)
{
    StateStack_.push_back(EYsonState::Terminated);
    switch (ysonType) {
        case EYsonType::Node:
            StateStack_.push_back(EYsonState::ExpectValue);
            break;
        case EYsonType::ListFragment:
            StateStack_.push_back(EYsonState::InsideListFragmentExpectValue);
            break;
        case EYsonType::MapFragment:
            StateStack_.push_back(EYsonState::InsideMapFragmentExpectKey);
            break;
        default:
            Y_UNREACHABLE();
    }
}

TStringBuf TYsonSyntaxChecker::StateExpectationString(EYsonState state)
{
    switch (state) {
        case EYsonState::Terminated:
            return "no further tokens (yson is completed)";

        case EYsonState::ExpectValue:
        case EYsonState::ExpectAttributelessValue:
        case EYsonState::InsideListFragmentExpectAttributelessValue:
        case EYsonState::InsideListFragmentExpectValue:
        case EYsonState::InsideMapFragmentExpectAttributelessValue:
        case EYsonState::InsideMapFragmentExpectValue:
        case EYsonState::InsideMapExpectAttributelessValue:
        case EYsonState::InsideMapExpectValue:
        case EYsonState::InsideAttributeMapExpectAttributelessValue:
        case EYsonState::InsideAttributeMapExpectValue:
        case EYsonState::InsideListExpectAttributelessValue:
        case EYsonState::InsideListExpectValue:
            return "value";

        case EYsonState::InsideMapFragmentExpectKey:
        case EYsonState::InsideMapExpectKey:
            return "key";
        case EYsonState::InsideAttributeMapExpectKey:
            return "attribute key";

        case EYsonState::InsideMapFragmentExpectEquality:
        case EYsonState::InsideMapExpectEquality:
        case EYsonState::InsideAttributeMapExpectEquality:
            return "=";

        case EYsonState::InsideListFragmentExpectSeparator:
        case EYsonState::InsideMapFragmentExpectSeparator:
        case EYsonState::InsideMapExpectSeparator:
        case EYsonState::InsideListExpectSeparator:
        case EYsonState::InsideAttributeMapExpectSeparator:
            return ";";
    }
    YCHECK(false);
}

void TYsonSyntaxChecker::ThrowUnexpectedToken(TStringBuf token)
{
    THROW_ERROR_EXCEPTION("Unexpected %Qv, expected %Qv",
        token,
        StateExpectationString(StateStack_.back()))
        << TErrorAttribute("yson_parser_state", StateStack_.back());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

bool operator==(TYsonItem lhs, TYsonItem rhs)
{
    if (lhs.GetType() != rhs.GetType()) {
        return false;
    }
    switch (lhs.GetType()) {
        case EYsonItemType::BooleanValue:
            return lhs.UncheckedAsBoolean() == rhs.UncheckedAsBoolean();
        case EYsonItemType::Int64Value:
            return lhs.UncheckedAsInt64() == rhs.UncheckedAsInt64();
        case EYsonItemType::Uint64Value:
            return lhs.UncheckedAsUint64() == rhs.UncheckedAsUint64();
        case EYsonItemType::DoubleValue:
            return lhs.UncheckedAsDouble() == rhs.UncheckedAsDouble();
        case EYsonItemType::StringValue:
            return lhs.UncheckedAsString() == rhs.UncheckedAsString();
        default:
            return true;
    }
}

////////////////////////////////////////////////////////////////////////////////

TYsonPullParser::TYsonPullParser(IZeroCopyInput* input, EYsonType ysonType)
    : StreamReader_(input)
    , Lexer_(StreamReader_)
    , SyntaxChecker_(ysonType)
{ }

ui64 TYsonPullParser::GetTotalReadSize() const
{
    return Lexer_.GetTotalReadSize();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
