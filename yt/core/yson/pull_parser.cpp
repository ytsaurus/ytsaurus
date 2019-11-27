#include "pull_parser.h"

#include "consumer.h"

#include "token_writer.h"

namespace NYT::NYson {

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

static void TransferMapOrAttributesImpl(TYsonPullParserCursor* cursor, IYsonConsumer* consumer);
static void TransferListImpl(TYsonPullParserCursor* cursor, IYsonConsumer* consumer);

static void TransferComplexValueImpl(TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
{
    while (true) {
        switch (cursor->GetCurrent().GetType()) {
            case EYsonItemType::BeginAttributes:
                consumer->OnBeginAttributes();
                TransferMapOrAttributesImpl(cursor, consumer);
                consumer->OnEndAttributes();
                continue;
            case EYsonItemType::BeginList:
                consumer->OnBeginList();
                TransferListImpl(cursor, consumer);
                consumer->OnEndList();
                return;
            case EYsonItemType::BeginMap:
                consumer->OnBeginMap();
                TransferMapOrAttributesImpl(cursor, consumer);
                consumer->OnEndMap();
                return;
            case EYsonItemType::EntityValue:
                consumer->OnEntity();
                cursor->Next();
                return;
            case EYsonItemType::BooleanValue:
                consumer->OnBooleanScalar(cursor->GetCurrent().UncheckedAsBoolean());
                cursor->Next();
                return;
            case EYsonItemType::Int64Value:
                consumer->OnInt64Scalar(cursor->GetCurrent().UncheckedAsInt64());
                cursor->Next();
                return;
            case EYsonItemType::Uint64Value:
                consumer->OnUint64Scalar(cursor->GetCurrent().UncheckedAsUint64());
                cursor->Next();
                return;
            case EYsonItemType::DoubleValue:
                consumer->OnDoubleScalar(cursor->GetCurrent().UncheckedAsDouble());
                cursor->Next();
                return;
            case EYsonItemType::StringValue:
                consumer->OnStringScalar(cursor->GetCurrent().UncheckedAsString());
                cursor->Next();
                return;

            case EYsonItemType::EndOfStream:
            case EYsonItemType::EndAttributes:
            case EYsonItemType::EndMap:
            case EYsonItemType::EndList:
                break;
        }
        YT_ABORT();
    }
}

static void TransferMapOrAttributesImpl(TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
{
    YT_ASSERT(cursor->GetCurrent().GetType() == EYsonItemType::BeginAttributes ||
        cursor->GetCurrent().GetType() == EYsonItemType::BeginMap);
    cursor->Next();
    while (cursor->GetCurrent().GetType() == EYsonItemType::StringValue) {
        consumer->OnKeyedItem(cursor->GetCurrent().UncheckedAsString());
        cursor->Next();
        TransferComplexValueImpl(cursor, consumer);
    }
    YT_ASSERT(cursor->GetCurrent().GetType() == EYsonItemType::EndAttributes ||
             cursor->GetCurrent().GetType() == EYsonItemType::EndMap);
    cursor->Next();
}

static void TransferListImpl(TYsonPullParserCursor* cursor, IYsonConsumer* consumer)
{
    YT_ASSERT(cursor->GetCurrent().GetType() == EYsonItemType::BeginList);
    cursor->Next();
    while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
        consumer->OnListItem();
        TransferComplexValueImpl(cursor, consumer);
    }
    cursor->Next();
}

////////////////////////////////////////////////////////////////////////////////

static void TransferMapOrAttributesImpl(TYsonPullParserCursor* cursor, TCheckedInDebugYsonTokenWriter* writer);
static void TransferListImpl(TYsonPullParserCursor* cursor, TCheckedInDebugYsonTokenWriter* writer);

Y_FORCE_INLINE static void TransferComplexValueImpl(TYsonPullParserCursor* cursor, TCheckedInDebugYsonTokenWriter* writer)
{
    while (true) {
        switch (cursor->GetCurrent().GetType()) {
            case EYsonItemType::BeginAttributes:
                writer->WriteBeginAttributes();
                TransferMapOrAttributesImpl(cursor, writer);
                writer->WriteEndAttributes();
                continue;
            case EYsonItemType::BeginList:
                writer->WriteBeginList();
                TransferListImpl(cursor, writer);
                writer->WriteEndList();
                return;
            case EYsonItemType::BeginMap:
                writer->WriteBeginMap();
                TransferMapOrAttributesImpl(cursor, writer);
                writer->WriteEndMap();
                return;
            case EYsonItemType::EntityValue:
                writer->WriteEntity();
                cursor->Next();
                return;
            case EYsonItemType::BooleanValue:
                writer->WriteBinaryBoolean(cursor->GetCurrent().UncheckedAsBoolean());
                cursor->Next();
                return;
            case EYsonItemType::Int64Value:
                writer->WriteBinaryInt64(cursor->GetCurrent().UncheckedAsInt64());
                cursor->Next();
                return;
            case EYsonItemType::Uint64Value:
                writer->WriteBinaryUint64(cursor->GetCurrent().UncheckedAsUint64());
                cursor->Next();
                return;
            case EYsonItemType::DoubleValue:
                writer->WriteBinaryDouble(cursor->GetCurrent().UncheckedAsDouble());
                cursor->Next();
                return;
            case EYsonItemType::StringValue:
                writer->WriteBinaryString(cursor->GetCurrent().UncheckedAsString());
                cursor->Next();
                return;

            case EYsonItemType::EndOfStream:
            case EYsonItemType::EndAttributes:
            case EYsonItemType::EndMap:
            case EYsonItemType::EndList:
                break;
        }
        YT_ABORT();
    }
}

static void TransferMapOrAttributesImpl(TYsonPullParserCursor* cursor, TCheckedInDebugYsonTokenWriter* writer)
{
    YT_ASSERT(cursor->GetCurrent().GetType() == EYsonItemType::BeginAttributes ||
        cursor->GetCurrent().GetType() == EYsonItemType::BeginMap);
    cursor->Next();
    while (cursor->GetCurrent().GetType() == EYsonItemType::StringValue) {
        writer->WriteBinaryString(cursor->GetCurrent().UncheckedAsString());
        writer->WriteKeyValueSeparator();
        cursor->Next();
        TransferComplexValueImpl(cursor, writer);
        writer->WriteItemSeparator();
    }
    YT_ASSERT(cursor->GetCurrent().GetType() == EYsonItemType::EndAttributes ||
             cursor->GetCurrent().GetType() == EYsonItemType::EndMap);
    cursor->Next();
}

static void TransferListImpl(TYsonPullParserCursor* cursor, TCheckedInDebugYsonTokenWriter* writer)
{
    YT_ASSERT(cursor->GetCurrent().GetType() == EYsonItemType::BeginList);
    cursor->Next();
    while (cursor->GetCurrent().GetType() != EYsonItemType::EndList) {
        TransferComplexValueImpl(cursor, writer);
        writer->WriteItemSeparator();
    }
    cursor->Next();
}

////////////////////////////////////////////////////////////////////////////////

TZeroCopyInputStreamReader::TZeroCopyInputStreamReader(IZeroCopyInput* reader)
    : Reader_(reader)
{ }

ui64 TZeroCopyInputStreamReader::GetTotalReadSize() const
{
    return TotalReadBlocksSize_ + (Current_ - Begin_);
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

TYsonItem TYsonPullParser::Next()
{
    try {
        Lexer_.CheckpointContext();
        return NextImpl();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error occurred while parsing YSON")
            << GetErrorAttributes()
            << ex;
    }
}

std::vector<TErrorAttribute> TYsonPullParser::GetErrorAttributes() const
{
    auto result = Lexer_.GetErrorAttributes();
    auto [context, contextPosition] = Lexer_.GetContextFromCheckpoint();
    TStringStream markedContext;
    markedContext << EscapeC(context.substr(0, contextPosition)) << "  ERROR>>>  " << EscapeC(context.substr(contextPosition));
    result.emplace_back("context", EscapeC(context));
    result.emplace_back("context_pos", contextPosition);
    result.emplace_back("marked_context", markedContext.Str());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TErrorAttribute> TYsonPullParserCursor::GetErrorAttributes() const
{
    return Parser_->GetErrorAttributes();
}

void TYsonPullParserCursor::SkipComplexValue()
{
    bool isAttributes = false;
    switch (Current_.GetType()) {
        case EYsonItemType::BeginAttributes:
            isAttributes = true;
            [[fallthrough]];
        case EYsonItemType::BeginList:
        case EYsonItemType::BeginMap: {
            const auto nestingLevel = Parser_->GetNestingLevel();
            while (Parser_->GetNestingLevel() >= nestingLevel) {
                Parser_->Next();
            }
            Current_ = Parser_->Next();
            if (isAttributes) {
                SkipComplexValue();
            }
            return;
        }
        case EYsonItemType::EntityValue:
        case EYsonItemType::BooleanValue:
        case EYsonItemType::Int64Value:
        case EYsonItemType::Uint64Value:
        case EYsonItemType::DoubleValue:
        case EYsonItemType::StringValue:
            Next();
            return;

        case EYsonItemType::EndOfStream:
        case EYsonItemType::EndAttributes:
        case EYsonItemType::EndMap:
        case EYsonItemType::EndList:
            YT_ABORT();
    }
    YT_ABORT();
}

void TYsonPullParserCursor::TransferComplexValue(NYT::NYson::IYsonConsumer* consumer)
{
    NDetail::TransferComplexValueImpl(this, consumer);
}

void TYsonPullParserCursor::TransferComplexValue(NYT::NYson::TCheckedInDebugYsonTokenWriter* writer)
{
    NDetail::TransferComplexValueImpl(this, writer);
}

////////////////////////////////////////////////////////////////////////////////

void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    const std::vector<EYsonItemType>& expected)
{
    YT_VERIFY(!expected.empty());
    TString expectedString;
    if (expected.size() > 1) {
        TStringStream out;
        out << "one of the tokens {";
        for (const auto& token : expected) {
            out << Format("%Qlv, ", token);
        }
        out << "}";
        expectedString = out.Str();
    } else {
        expectedString = Format("%Qlv", expected[0]);
    }

    THROW_ERROR_EXCEPTION("Cannot parse %Qv; expected: %v; actual: %Qlv",
        description,
        expectedString,
        cursor->GetType())
        << cursor.GetErrorAttributes();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
