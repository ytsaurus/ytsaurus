#include "pull_parser.h"

#include "consumer.h"

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
        auto [context, contextPosition] = Lexer_.GetContextFromCheckpoint();
        TStringStream markedContext;
        markedContext << EscapeC(context.substr(0, contextPosition)) << "  ERROR>>>  " << EscapeC(context.substr(contextPosition));
        THROW_ERROR_EXCEPTION("Error occurred while parsing YSON")
            << Lexer_
            << TErrorAttribute("context", EscapeC(context))
            << TErrorAttribute("context_pos", contextPosition)
            << TErrorAttribute("marked_context", markedContext.Str())
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TYsonPullParserCursor::SkipComplexValue()
{
    bool isAttributes = false;
    switch (Current_.GetType()) {
        case EYsonItemType::BeginAttributes:
            isAttributes = true;
            // fallthrough
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
