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

class TYsonItemCreatingVisitor
{
public:
    using TResult = TYsonItem;

public:
    TYsonItem OnBeginAttributes()
    {
        return TYsonItem::Simple(EYsonItemType::BeginAttributes);
    }

    TYsonItem OnEndAttributes()
    {
        return TYsonItem::Simple(EYsonItemType::EndAttributes);
    }

    TYsonItem OnBeginMap()
    {
        return TYsonItem::Simple(EYsonItemType::BeginMap);
    }

    TYsonItem OnEndMap()
    {
        return TYsonItem::Simple(EYsonItemType::EndMap);
    }

    TYsonItem OnBeginList()
    {
        return TYsonItem::Simple(EYsonItemType::BeginList);
    }

    TYsonItem OnEndList()
    {
        return TYsonItem::Simple(EYsonItemType::EndList);
    }

    TYsonItem OnString(TStringBuf value)
    {
        return TYsonItem::String(value);
    }

    TYsonItem OnInt64(i64 value)
    {
        return TYsonItem::Int64(value);
    }

    TYsonItem OnUint64(ui64 value)
    {
        return TYsonItem::Uint64(value);
    }

    TYsonItem OnDouble(double value)
    {
        return TYsonItem::Double(value);
    }

    TYsonItem OnBoolean(bool value)
    {
        return TYsonItem::Boolean(value);
    }

    TYsonItem OnEntity()
    {
        return TYsonItem::Simple(EYsonItemType::EntityValue);
    }

    TYsonItem OnEndOfStream()
    {
        return TYsonItem::Simple(EYsonItemType::EndOfStream);
    }

    void OnEquality()
    { }

    void OnSeparator()
    { }
};

class TNullVisitor
{
public:
    using TResult = void;

public:
    void OnBeginAttributes()
    { }

    void OnEndAttributes()
    { }

    void OnBeginMap()
    { }

    void OnEndMap()
    { }

    void OnBeginList()
    { }

    void OnEndList()
    { }

    void OnString(TStringBuf /* value */)
    { }

    void OnInt64(i64 /* value */)
    { }

    void OnUint64(ui64 /* value */)
    { }

    void OnDouble(double /* value */)
    { }

    void OnBoolean(bool /* value */)
    { }

    void OnEntity()
    { }

    void OnEndOfStream()
    { }

    void OnEquality()
    { }

    void OnSeparator()
    { }
};

class TYsonTokenWritingVisitor
{
public:
    using TResult = void;

public:
    TYsonTokenWritingVisitor(TCheckedInDebugYsonTokenWriter* writer)
        : Writer_(writer)
    { }

    void OnBeginAttributes()
    {
        Writer_->WriteBeginAttributes();
    }

    void OnEndAttributes()
    {
        Writer_->WriteEndAttributes();
    }

    void OnBeginMap()
    {
        Writer_->WriteBeginMap();
    }

    void OnEndMap()
    {
        Writer_->WriteEndMap();
    }

    void OnBeginList()
    {
        Writer_->WriteBeginList();
    }

    void OnEndList()
    {
        Writer_->WriteEndList();
    }

    void OnString(TStringBuf value)
    {
        Writer_->WriteBinaryString(value);
    }

    void OnInt64(i64 value)
    {
        Writer_->WriteBinaryInt64(value);
    }

    void OnUint64(ui64 value)
    {
        Writer_->WriteBinaryUint64(value);
    }

    void OnDouble(double value)
    {
        Writer_->WriteBinaryDouble(value);
    }

    void OnBoolean(bool value)
    {
        Writer_->WriteBinaryBoolean(value);
    }

    void OnEntity()
    {
        Writer_->WriteEntity();
    }

    void OnEndOfStream()
    { }

    void OnEquality()
    {
        Writer_->WriteKeyValueSeparator();
    }

    void OnSeparator()
    {
        Writer_->WriteItemSeparator();
    }

private:
    TCheckedInDebugYsonTokenWriter* const Writer_;
};

TYsonItem TYsonPullParser::Next()
{
    try {
        Lexer_.CheckpointContext();
        return NextImpl(TYsonItemCreatingVisitor());
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error occurred while parsing YSON")
            << GetErrorAttributes()
            << ex;
    }
}

void TYsonPullParser::SkipComplexValue()
{
    TraverseComplexValueOrAttributes(TNullVisitor(), /* stopAfterAttributes */ false);
}

void TYsonPullParser::TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer)
{
    TraverseComplexValueOrAttributes(TYsonTokenWritingVisitor(writer), /* stopAfterAttributes */ false);
}

void TYsonPullParser::SkipComplexValue(const TYsonItem& previousItem)
{
    TraverseComplexValueOrAttributes(TNullVisitor(), previousItem, /* stopAfterAttributes */ false);
}

void TYsonPullParser::TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer, const TYsonItem& previousItem)
{
    TraverseComplexValueOrAttributes(TYsonTokenWritingVisitor(writer), previousItem, /* stopAfterAttributes */ false);
}

void TYsonPullParser::SkipAttributes(const TYsonItem& previousItem)
{
    EnsureYsonToken("attributes", *this, previousItem, EYsonItemType::BeginAttributes);
    TraverseComplexValueOrAttributes(TNullVisitor(), previousItem, /* stopAfterAttributes */ true);
}

void TYsonPullParser::SkipComplexValueOrAttributes(const TYsonItem& previousItem)
{
    TraverseComplexValueOrAttributes(TNullVisitor(), previousItem, /* stopAfterAttributes */ true);
}

void TYsonPullParser::TransferAttributes(TCheckedInDebugYsonTokenWriter* writer, const TYsonItem& previousItem)
{
    EnsureYsonToken("attributes", *this, previousItem, EYsonItemType::BeginAttributes);
    TraverseComplexValueOrAttributes(TYsonTokenWritingVisitor(writer), previousItem, /* stopAfterAttributes */ true);
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
    Parser_->SkipComplexValue(Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::TransferComplexValue(NYT::NYson::IYsonConsumer* consumer)
{
    NDetail::TransferComplexValueImpl(this, consumer);
}

void TYsonPullParserCursor::TransferComplexValue(NYT::NYson::TCheckedInDebugYsonTokenWriter* writer)
{
    Parser_->TransferComplexValue(writer, Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::SkipAttributes()
{
    Parser_->SkipAttributes(Current_);
    Current_ = Parser_->Next();
}

void TYsonPullParserCursor::TransferAttributes(NYT::NYson::IYsonConsumer* consumer)
{
    EnsureYsonToken("attributes", *this, EYsonItemType::BeginAttributes);
    consumer->OnBeginAttributes();
    NDetail::TransferMapOrAttributesImpl(this, consumer);
    consumer->OnEndAttributes();
}

void TYsonPullParserCursor::TransferAttributes(NYT::NYson::TCheckedInDebugYsonTokenWriter* writer)
{
    Parser_->TransferAttributes(writer, Current_);
    Current_ = Parser_->Next();
}

////////////////////////////////////////////////////////////////////////////////

TString CreateExpectedItemTypesString(const std::vector<EYsonItemType>& expected)
{
    YT_VERIFY(!expected.empty());
    if (expected.size() > 1) {
        TStringStream out;
        out << "one of the tokens {";
        for (const auto& token : expected) {
            out << Format("%Qlv, ", token);
        }
        out << "}";
        return out.Str();
    } else {
        return Format("%Qlv", expected[0]);
    }
}

void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    const std::vector<EYsonItemType>& expected)
{
    THROW_ERROR_EXCEPTION("Cannot parse %Qv; expected: %v; actual: %Qlv",
        description,
        CreateExpectedItemTypesString(expected),
        item.GetType())
        << parser.GetErrorAttributes();
}

void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    const std::vector<EYsonItemType>& expected)
{
    THROW_ERROR_EXCEPTION("Cannot parse %Qv; expected: %v; actual: %Qlv",
        description,
        CreateExpectedItemTypesString(expected),
        cursor->GetType())
        << cursor.GetErrorAttributes();
}

void ThrowUnexpectedTokenException(
    TStringBuf description,
    const TYsonPullParser& parser,
    const TYsonItem& item,
    EYsonItemType expected,
    bool optional)
{
    std::vector<EYsonItemType> allExpected = {expected};
    if (optional) {
        allExpected.push_back(EYsonItemType::EntityValue);
    }

    auto fullDescription = TString(optional ? "optional " : "") + description;
    ThrowUnexpectedYsonTokenException(
        fullDescription,
        parser,
        item,
        allExpected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
