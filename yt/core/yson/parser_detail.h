#pragma once

#include "detail.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

template <class TConsumer, class TBlockStream, bool EnableLinePositionInfo>
class TParser
    : public TLexerBase<TBlockStream, EnableLinePositionInfo>
{
private:
    typedef TLexerBase<TBlockStream, EnableLinePositionInfo> TBase;
    TConsumer* Consumer;

public:
    TParser(const TBlockStream& blockStream, TConsumer* consumer, TNullable<i64> memoryLimit) 
        : TBase(blockStream, memoryLimit)
        , Consumer(consumer)
    { }
    
    void DoParse(EYsonType ParsingMode)
    {
        switch (ParsingMode) {
            case EYsonType::Node:
                ParseNode<true>();
                break;

            case EYsonType::ListFragment:
                ParseListFragment<true>(EndSymbol);
                break;

            case EYsonType::MapFragment:
                ParseMapFragment<true>(EndSymbol);
                break;

            default:
                YUNREACHABLE();
        }

        while (!(TBase::IsFinished() && TBase::IsEmpty())) {
            if (TBase::template SkipSpaceAndGetChar<true>() != EndSymbol) {
                THROW_ERROR_EXCEPTION("Stray %Qv found",
                    *TBase::Begin())
                    << *this;
            } else if (!TBase::IsEmpty()) {
                TBase::Advance(1);
            }
        }
    }

    /// Parse routines
    void ParseAttributes()
    {
        Consumer->OnBeginAttributes();
        ParseMapFragment(EndAttributesSymbol);
        TBase::SkipCharToken(EndAttributesSymbol);
        Consumer->OnEndAttributes();
    }

    void ParseMap()
    {
        Consumer->OnBeginMap();
        ParseMapFragment(EndMapSymbol); 
        TBase::SkipCharToken(EndMapSymbol);
        Consumer->OnEndMap();
    }
    
    void ParseList()
    {
        Consumer->OnBeginList();
        ParseListFragment(EndListSymbol);
        TBase::SkipCharToken(EndListSymbol);
        Consumer->OnEndList();
    }

    template <bool AllowFinish>
    void ParseNode()
    {
        return ParseNode<AllowFinish>(TBase::SkipSpaceAndGetChar());
    }

    template <bool AllowFinish>
    void ParseNode(char ch)
    {
        if (ch == BeginAttributesSymbol) {
            TBase::Advance(1);
            ParseAttributes();
            ch = TBase::SkipSpaceAndGetChar();
        }
        
        switch (ch) { 
            case BeginMapSymbol: 
                TBase::Advance(1);
                ParseMap(); 
                break;

            case BeginListSymbol: 
                TBase::Advance(1);
                ParseList();
                break;

            case '"': {
                TBase::Advance(1);
                TStringBuf value;
                TBase::ReadQuotedString(&value);
                Consumer->OnStringScalar(value);
                break;
            }
            case StringMarker: {
                TBase::Advance(1);
                TStringBuf value;
                TBase::ReadBinaryString(&value);
                Consumer->OnStringScalar(value);
                break;
            }
            case Int64Marker:{
                TBase::Advance(1);
                i64 value;
                TBase::ReadBinaryInt64(&value);
                Consumer->OnInt64Scalar(value);
                break;
            }
            case Uint64Marker:{
                TBase::Advance(1);
                ui64 value;
                TBase::ReadBinaryUint64(&value);
                Consumer->OnUint64Scalar(value);
                break;
            }
            case DoubleMarker: {
                TBase::Advance(1);
                double value;
                TBase::ReadBinaryDouble(&value);
                Consumer->OnDoubleScalar(value);
                break;
            }
            case FalseMarker: {
                TBase::Advance(1);
                Consumer->OnBooleanScalar(false);
                break;
            }
            case TrueMarker: {
                TBase::Advance(1);
                Consumer->OnBooleanScalar(true);
                break;
            }
            case EntitySymbol:
                TBase::Advance(1);
                Consumer->OnEntity();
                break;

            default: {
                if (isdigit(ch) || ch == '-' || ch == '+') { // case of '+' is handled in AfterPlus state
                    ReadNumeric<AllowFinish>();
                } else if (isalpha(ch) || ch == '_') {
                    TStringBuf value;
                    TBase::template ReadUnquotedString<AllowFinish>(&value);
                    Consumer->OnStringScalar(value);
                } else if (ch == '%') {
                    TBase::Advance(1);
                    Consumer->OnBooleanScalar(TBase::template ReadBoolean<AllowFinish>());
                } else {
                    THROW_ERROR_EXCEPTION("Unexpected %Qv while parsing node",
                        ch)
                        << *this;
                }
            }
        }
    }

    void ParseKey()
    {
        return ParseKey(TBase::SkipSpaceAndGetChar());
    }

    void ParseKey(char ch)
    {        
        switch (ch) { 
            case '"': {
                TBase::Advance(1);
                TStringBuf value;
                TBase::ReadQuotedString(&value);
                Consumer->OnKeyedItem(value);
                break;
            }
            case StringMarker: {
                TBase::Advance(1);
                TStringBuf value;
                TBase::ReadBinaryString(&value);
                Consumer->OnKeyedItem(value);
                break;
            }
            default: {
                if (isalpha(ch) || ch == '_') {
                    TStringBuf value;
                    TBase::ReadUnquotedString(&value);
                    Consumer->OnKeyedItem(value);
                } else {
                    THROW_ERROR_EXCEPTION("Unexpected %Qv while parsing key",
                        ch)
                        << *this;
                }
            }
        }
    }
    
    template <bool AllowFinish>
    void ParseMapFragment(char endSymbol)
    {
        char ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
        while (ch != endSymbol) {
            ParseKey(ch);
            ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            if (ch == KeyValueSeparatorSymbol) {
                TBase::Advance(1);
            } else {
                THROW_ERROR_EXCEPTION("Expected %Qv but %Qv found",
                    KeyValueSeparatorSymbol,
                    ch)
                    << *this;
            }
            ParseNode<AllowFinish>();
            ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            if (ch == KeyedItemSeparatorSymbol) {
                TBase::Advance(1);
                ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            } else if (ch != endSymbol) {
                THROW_ERROR_EXCEPTION("Expected %Qv or %Qv but %Qv found",
                    KeyedItemSeparatorSymbol,
                    endSymbol,
                    ch)
                    << *this;
            }
            
        }
    }

    void ParseMapFragment(char endSymbol)
    {
        ParseMapFragment<false>(endSymbol);
    }

    template <bool AllowFinish>
    void ParseListFragment(char endSymbol)
    {
        char ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
        while (ch != endSymbol) {
            Consumer->OnListItem();
            ParseNode<AllowFinish>(ch);
            ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            if (ch == ListItemSeparatorSymbol) {
                TBase::Advance(1);
                ch = TBase::template SkipSpaceAndGetChar<AllowFinish>();
            } else if (ch != endSymbol) {
                THROW_ERROR_EXCEPTION("Expected %Qv or %Qv but %Qv found",
                    ListItemSeparatorSymbol,
                    endSymbol,
                    ch)
                    << *this;
            }            
        }
    }

    void ParseListFragment(char endSymbol)
    {
        ParseListFragment<false>(endSymbol);
    }
    
    template <bool AllowFinish>
    void ReadNumeric()
    {
        TStringBuf valueBuffer;
        ENumericResult numericResult = TBase::template ReadNumeric<AllowFinish>(&valueBuffer);

        if (numericResult == ENumericResult::Double) {
            double value;
            try {
                value = FromString<double>(valueBuffer);
            } catch (const std::exception& ex) {
                // This exception is wrapped in parser.
                THROW_ERROR_EXCEPTION("Failed to parse double literal %Qv",
                    valueBuffer)
                    << *this
                    << ex;
            }
            Consumer->OnDoubleScalar(value);
        } else if (numericResult == ENumericResult::Int64) {
            i64 value;
            try {
                value = FromString<i64>(valueBuffer);
            } catch (const std::exception& ex) {
                // This exception is wrapped in parser.
                THROW_ERROR_EXCEPTION("Failed to parse int64 literal %Qv",
                    valueBuffer)
                    << *this
                    << ex;
            }
            Consumer->OnInt64Scalar(value);
        } else if (numericResult == ENumericResult::Uint64) {
            ui64 value;
            try {
                value = FromString<ui64>(valueBuffer.SubStr(0, valueBuffer.size() - 1));
            } catch (const std::exception& ex) {
                // This exception is wrapped in parser.
                THROW_ERROR_EXCEPTION("Failed to parse uint64 literal %Qv",
                    valueBuffer)
                    << *this
                    << ex;
            }
            Consumer->OnUint64Scalar(value);
        }
    } 
};

////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail

template <class TConsumer, class TBlockStream>
void ParseYsonStreamImpl(
    const TBlockStream& blockStream, 
    IYsonConsumer* consumer, 
    EYsonType parsingMode, 
    bool enableLinePositionInfo,
    TNullable<i64> memoryLimit)
{
    if (enableLinePositionInfo) {
        typedef NDetail::TParser<TConsumer, TBlockStream, true> TImpl;
        TImpl impl(blockStream, consumer, memoryLimit);
        impl.DoParse(parsingMode);
    } else {
        typedef NDetail::TParser<TConsumer, TBlockStream, false> TImpl;
        TImpl impl(blockStream, consumer, memoryLimit);
        impl.DoParse(parsingMode);
    }
}

class TStatelessYsonParserImplBase
{
public:
    virtual void Parse(const TStringBuf& data, EYsonType type = EYsonType::Node) = 0;

    virtual ~TStatelessYsonParserImplBase()
    { }
};

template <class TConsumer, bool EnableLinePositionInfo>
class TStatelessYsonParserImpl
    : public TStatelessYsonParserImplBase
{
private:
    typedef NDetail::TParser<TConsumer, TStringReader, EnableLinePositionInfo> TParser;
    TParser Parser;

public:
    TStatelessYsonParserImpl(TConsumer* consumer, TNullable<i64> memoryLimit)
        : Parser(TStringReader(), consumer, memoryLimit)
    { }
    
    void Parse(const TStringBuf& data, EYsonType type = EYsonType::Node) override
    {
        Parser.SetBuffer(data.begin(), data.end());
        Parser.DoParse(type);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
