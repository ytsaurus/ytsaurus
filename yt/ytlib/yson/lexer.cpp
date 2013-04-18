#include "stdafx.h"
#include "lexer.h"

#include "token.h"
#include "zigzag.h"
#include "varint.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/property.h>

#include <util/string/escape.h>

#include "yson_lexer_detail.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TLexerImpl
{
private:
    // EReadStartCase tree representation:
    // Root                                =     xb
    //     BinaryStringOrOtherSpecialToken =    x0b
    //         BinaryString                =    00b
    //         OtherSpecialToken           =    10b
    //     Other                           =    x1b
    //         BinaryIntegerOrBinaryDouble =   x01b
    //             BinaryInteger           =   001b
    //             BinaryDouble            =   101b
    //         Other                       = xxx11b
    //             Quote                   = 00011b
    //             DigitOrMinus            = 00111b
    //             String                  = 01011b
    //             Space                   = 01111b
    //             Plus                    = 10011b
    //             None                    = 10111b
    DECLARE_ENUM(EReadStartCase,
        ((BinaryString)                 (0))    // =    00b
        ((OtherSpecialToken)            (2))    // =    10b

        ((BinaryInteger)                (1))    // =   001b
        ((BinaryDouble)                 (5))    // =   101b

        ((Quote)                        (3))    // = 00011b
        ((DigitOrMinus)                 (7))    // = 00111b
        ((String)                       (11))   // = 01011b
        ((Space)                        (15))   // = 01111b
        ((Plus)                         (19))   // = 10011b
        ((None)                         (23))   // = 10111b
    );

    static EReadStartCase GetStartState(char ch)
    {
#define NN EReadStartCase::None
#define BS EReadStartCase::BinaryString	
#define BI EReadStartCase::BinaryInteger	
#define BD EReadStartCase::BinaryDouble	
#define SP EReadStartCase::Space
#define DM EReadStartCase::DigitOrMinus
#define ST EReadStartCase::String
#define PL EReadStartCase::Plus
#define QU EReadStartCase::Quote
	
        static const ui8 lookupTable[] = 
        {
            NN,BS,BI,BD,NN,NN,NN,NN,NN,SP,SP,SP,SP,SP,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            // 32
            SP,NN,QU,
            ETokenType::Hash << 2 | EReadStartCase::OtherSpecialToken, // '#'
            NN,
            ST,NN,NN,
            ETokenType::LeftParenthesis << 2 | EReadStartCase::OtherSpecialToken, // '('
            ETokenType::RightParenthesis << 2 | EReadStartCase::OtherSpecialToken, // ')'
            NN,
            PL, // '+'
            ETokenType::Comma << 2 | EReadStartCase::OtherSpecialToken, // ','
            DM,NN,NN,
            // 48
            DM,DM,DM,DM,DM,DM,DM,DM,DM,DM,
            ETokenType::Colon << 2 | EReadStartCase::OtherSpecialToken, // ':'
            ETokenType::Semicolon << 2 | EReadStartCase::OtherSpecialToken, // ';'
            ETokenType::LeftAngle << 2 | EReadStartCase::OtherSpecialToken, // '<'
            ETokenType::Equals << 2 | EReadStartCase::OtherSpecialToken, // '='
            ETokenType::RightAngle << 2 | EReadStartCase::OtherSpecialToken, // '>'
            NN,
            
            // 64
            NN,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,
            
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,
            ETokenType::LeftBracket << 2 | EReadStartCase::OtherSpecialToken, // '['
            NN,
            ETokenType::RightBracket << 2 | EReadStartCase::OtherSpecialToken, // ']'
            NN,ST,
            // 96
            NN,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,
            
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,
            ETokenType::LeftBrace << 2 | EReadStartCase::OtherSpecialToken, // '{'
            NN,
            ETokenType::RightBrace << 2 | EReadStartCase::OtherSpecialToken, // '}'
            NN,NN,
            // 128
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,

            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN
        };
		
#undef NN
#undef BS
#undef BI
#undef BD
#undef SP
#undef DM
#undef ST
#undef PL
#undef QU
        return EReadStartCase(lookupTable[static_cast<ui8>(ch)]);
    }

    static bool IsSpaceFast(char ch)
    {
        static const ui8 lookupTable[] = 
        {
            0,0,0,0,0,0,0,0,0,1,1,1,1,1,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
     
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,

            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
        };
        return lookupTable[static_cast<ui8>(ch)];
    }

public:
    // Public state EState is encoded in four least significant bits of EInnerState.
    // So, we can get EState by applying mask 0xf to EInnerState.
    DECLARE_ENUM(EInnerState,
        ((None)                 (0x00 | TLexer::EState::None))
        ((Terminal)             (0x10 | TLexer::EState::Terminal))
        ((InsideBinaryInteger)  (0x20 | TLexer::EState::InProgress))
        ((InsideBinaryDouble)   (0x30 | TLexer::EState::InProgress))
        ((InsideBinaryString)   (0x40 | TLexer::EState::InProgress))
        ((InsideUnquotedString) (0x50 | TLexer::EState::InProgress))
        ((InsideQuotedString)   (0x60 | TLexer::EState::InProgress))
        ((InsideNumeric)        (0x70 | TLexer::EState::InProgress))
        ((InsideDouble)         (0x80 | TLexer::EState::InProgress))
        ((AfterPlus)            (0x90 | TLexer::EState::InProgress))
    );

    TLexerImpl()
    {
        Reset();
        TokenBuffer.reserve(StringBufferSize);
    }

    void Reset()
    {
        InnerState = EInnerState::None;
        Token.Reset();
        TokenBuffer.clear();
        BytesRead = 0;
    }

    const TToken& GetToken() const
    {
        YASSERT(InnerState == EInnerState::Terminal);
        return Token;
    }
    
    size_t Read(const TStringBuf& input)
    {
        auto begin = input.begin();
        auto end = input.end();

        if (InnerState == EInnerState::None) { 
            return ReadStart(begin, end) - begin;
        }

        switch (InnerState) {
            case EInnerState::InsideUnquotedString:
                return ReadUnquotedString(begin, end) - begin;

            case EInnerState::InsideQuotedString:
                return ReadQuotedString(begin, end) - begin;

            case EInnerState::InsideBinaryString:
                return ReadBinaryString(begin, end) - begin;

            case EInnerState::InsideNumeric:
                return ReadNumeric(begin, end) - begin;

            case EInnerState::InsideDouble:
                return ReadDouble(begin, end) - begin;

            case EInnerState::InsideBinaryInteger:
                return ReadBinaryInteger(begin, end) - begin;

            case EInnerState::InsideBinaryDouble:
                return ReadBinaryDouble(begin, end) - begin;

            case EInnerState::AfterPlus:
                return ReadAfterPlus(begin, end) - begin;

            default:
                // Should not consume chars in terminal states
                YUNREACHABLE();
        }
    }

    void Finish()
    {
        switch (InnerState) {
            case EInnerState::InsideBinaryInteger:
            case EInnerState::InsideBinaryDouble:
            case EInnerState::InsideBinaryString:
            case EInnerState::InsideQuotedString:
                THROW_ERROR_EXCEPTION("Premature end of stream (LexerState: %s, BytesRead: %d)",
                    ~InnerState.ToString(),
                    BytesRead);

            case EInnerState::InsideUnquotedString:
                Token.StringValue = GetBufferAsStringBuf();
                FinishString();
                break;

            case EInnerState::InsideNumeric:
                FinishNumeric();
                break;

            case EInnerState::InsideDouble:
                FinishDouble();
                break;

            case EInnerState::AfterPlus:
                FinishPlus();
                break;

            default:
                break;
        }
    }

    TLexer::EState GetState() const
    {
        return TLexer::EState(InnerState & 0xf);
    }

private:
    static const int StringBufferSize = 1 << 16;
    
    const char* ReadStartHelper(EReadStartCase state, const char* current, const char* end)
    {
        char ch = *current;

        // EReadStartCase tree representation:
        // ...
        //     Other                           =    x1b
        //         BinaryIntegerOrBinaryDouble =   x01b
        //             BinaryInteger           =   001b
        //             BinaryDouble            =   101b
        //         Other                       = xxx11b
        //             Quote                   = 00011b
        //             DigitOrMinus            = 00111b
        //             String                  = 01011b
        //             Space                   = 01111b
        //             Plus                    = 10011b
        //             None                    = 10111b
        if (state & 1 << 1) { // Other = xxx11b
            if (state == EReadStartCase::Quote) {
                ++current;
                SetInProgressState(EInnerState::InsideQuotedString);
                YASSERT(TokenBuffer.empty());
                YASSERT(BytesRead == 0);
                return ReadQuotedString(current, end);
            } else if (state == EReadStartCase::DigitOrMinus) { // case of '+' is handled in AfterPlus state
                ++current;
                YASSERT(TokenBuffer.empty());
                TokenBuffer.push_back(ch);
                SetInProgressState(EInnerState::InsideNumeric);
                return ReadNumeric(current, end);
            } else if (state == EReadStartCase::String) {
                ++current;
                YASSERT(TokenBuffer.empty());
                TokenBuffer.push_back(ch);
                SetInProgressState(EInnerState::InsideUnquotedString);
                return ReadUnquotedString(current, end);
            } else if (state == EReadStartCase::Space) {
                while (current != end && IsSpaceFast(*current)) {
                    ++current;
                }
                return ReadStart(current, end);
            } else if (state == EReadStartCase::Plus) {
                ++current;
                SetInProgressState(EInnerState::AfterPlus);
                return ReadAfterPlus(current, end);
            } else { // None
                YASSERT(state == EReadStartCase::None);
                THROW_ERROR_EXCEPTION("Unexpected character %s",
                    ~Stroka(ch).Quote());
            }
        } else { // BinaryIntegerOrBinaryDouble = x01b
            if (state & 1 << 2) { // BinaryDouble = 101b
                YASSERT(state == EReadStartCase::BinaryDouble);
                ++current;
                SetInProgressState(EInnerState::InsideBinaryDouble);
                YASSERT(Token.DoubleValue == 0.0);
                YASSERT(BytesRead == 0);
                BytesRead = -static_cast<int>(sizeof(double));
                return ReadBinaryDouble(current, end);
            } else { // BinaryInteger = 001b
                YASSERT(state == EReadStartCase::BinaryInteger);
                ++current;
                SetInProgressState(EInnerState::InsideBinaryInteger);
                YASSERT(Token.IntegerValue == 0);
                YASSERT(BytesRead == 0);
                return ReadBinaryInteger(current, end);
            }
        }


    }

    const char* ReadStart(const char* begin, const char* end)
    {
        const char* current = begin;

        if (current == end) {
            return current;
        }
        char ch = *current;
        auto state = GetStartState(ch);

        // EReadStartCase tree representation:
        // Root                                =     xb
        //     BinaryStringOrOtherSpecialToken =    x0b
        //         BinaryString                =    00b
        //         OtherSpecialToken           =    10b
        //     Other                           =    x1b
        //         ...
        if (state & 1) { // Other = x1b
            return ReadStartHelper(state, current, end);
        } else { // BinaryStringOrOtherSpecialToken = x0b
            ++current;
            if (state & 1 << 1) { // OtherSpecialToken = 10b
                YASSERT((state & 3) == EReadStartCase::OtherSpecialToken);
                ProduceToken(ETokenType(state >> 2));
                return current;
            } else { // BinaryString = 00b
                YASSERT((state & 3) == EReadStartCase::BinaryString);
                SetInProgressState(EInnerState::InsideBinaryString);
                YASSERT(TokenBuffer.empty());
                YASSERT(BytesRead == 0);
                return ReadBinaryString(current, end);
            }
        }
    }
    
    const char* ReadUnquotedString(const char* begin, const char* end)
    {
        for (auto current = begin; current != end; ++current) {
            char ch = *current;
            if (isalpha(ch) || isdigit(ch) ||
                ch == '_' || ch == '-' || ch == '%' || ch == '.')
            {
                TokenBuffer.push_back(ch);
            } else {
                Token.StringValue = GetBufferAsStringBuf();
                FinishString();
                return current;
            }
        }
        return end;
    }

    const char* ReadQuotedString(const char* begin, const char* end)
    {
        for (auto current = begin; current != end; ++current) {
            bool finish = false;
            char ch = *current;
            if (ch != '"') {
                TokenBuffer.push_back(ch);
            } else {
                // We must count the number of '\' at the end of StringValue
                // to check if it's not \"
                int slashCount = 0;
                int length = TokenBuffer.size();
                while (slashCount < length && TokenBuffer[length - 1 - slashCount] == '\\')
                    ++slashCount;
                if (slashCount % 2 == 0) {
                    finish = true;
                } else {
                    TokenBuffer.push_back(ch);
                }
            }

            if (finish) {
                // TODO(babenko): consider optimizing
                auto unquotedValue = UnescapeC(GetBufferAsString());
                TokenBuffer.clear();
                TokenBuffer.insert(TokenBuffer.end(), &*unquotedValue.begin(), &*unquotedValue.begin() + unquotedValue.size());
                Token.StringValue = GetBufferAsStringBuf();
                FinishString();
                return ++current;
            } else {
                ++BytesRead;
            }
        }
        return end;
    }

    const char* ReadBinaryInteger(const char* begin, const char* end)
    {
        ui64 ui64Value = static_cast<ui64>(Token.IntegerValue);
        for (auto current = begin; current != end; ++current) {
            ui8 byte = static_cast<ui8>(*current);

            if (7 * BytesRead > 8 * sizeof(ui64) ) {
                THROW_ERROR_EXCEPTION("The data is too long to read binary Integer");
            }
            

            ui64Value |= (static_cast<ui64> (byte & 0x7F)) << (7 * BytesRead);
            ++BytesRead;

            if ((byte & 0x80) == 0) {
                Token.IntegerValue = ZigZagDecode64(static_cast<ui64>(ui64Value));
                ProduceToken(ETokenType::Integer);
                BytesRead = 0;
                return ++current;
            }
        }
        Token.IntegerValue = static_cast<i64>(ui64Value);
        return end;
    }
    
    const char* ReadBinaryString(const char* begin, const char* end)
    {
        // Reading length
        if (BytesRead >= 0) {
            begin = ReadBinaryInteger(begin, end);
            if (GetState() == TLexer::EState::Terminal) {
                i64 length = Token.IntegerValue;
                if (length < 0) {
                    THROW_ERROR_EXCEPTION("Error reading binary string: String cannot have negative length (Length: %" PRId64 ")",
                        length);
                }
                // Token.IntegerValue = 0; // It's not necessary
                YASSERT(Token.StringValue.empty());
                SetInProgressState(EInnerState::InsideBinaryString);
                BytesRead = -length;
            } else {
                YASSERT(begin == end);
                return begin;
            }
        }

        if (LIKELY(begin != end)) {
            int length = -BytesRead;
            YASSERT(length >= 0);
            bool enough = end >= begin + length;

            // performance hack
            if (enough && TokenBuffer.empty()) {
                Token.StringValue = TStringBuf(begin, length);
                FinishString();
                return begin + length;
            }

            if (enough) {
                end = begin + length;
            } else {
                length = end - begin;
            }

            TokenBuffer.insert(TokenBuffer.end(), begin, end);
            BytesRead += length;
        }

        if (BytesRead == 0) {
            Token.StringValue = GetBufferAsStringBuf();
            FinishString();
        }

        return end;
    }

    const char* ReadNumeric(const char* begin, const char* end)
    {
        for (auto current = begin; current != end; ++current) {
            char ch = *current;
            if (isdigit(ch) || ch == '+' || ch == '-') { // Seems like it can't be '+' or '-'
                TokenBuffer.push_back(ch);
            } else if (ch == '.' || ch == 'e' || ch == 'E') {
                TokenBuffer.push_back(ch);
                InnerState = EInnerState::InsideDouble;
                return ReadDouble(++current, end);
            } else if (isalpha(ch)) {
                THROW_ERROR_EXCEPTION("Unexpected character in numeric (Char: %s, Token: %s)",
                    ~Stroka(ch).Quote(),
                    ~GetBufferAsString());
            } else {
                FinishNumeric();
                return current;
            }
        }
        return end;
    }

    const char* ReadDouble(const char* begin, const char* end)
    {
        for (auto current = begin; current != end; ++current) {
            char ch = *current;
            if (isdigit(ch) ||
                ch == '+' || ch == '-' ||
                ch == '.' ||
                ch == 'e' || ch == 'E')
            {
                TokenBuffer.push_back(ch);
            } else if (isalpha(ch)) {
                THROW_ERROR_EXCEPTION("Unexpected character in numeric (Char: %s, Token: %s)",
                    ~Stroka(ch).Quote(),
                    ~GetBufferAsString());
            } else {
                FinishDouble();
                return current;
            }
        }
        return end;
    }

    const char* ReadBinaryDouble(const char* begin, const char* end)
    {
        YASSERT(BytesRead <= 0);
        if (end - begin > -BytesRead) {
            end = begin - BytesRead;
        }

        if (begin != end) {
            std::copy(begin, end, reinterpret_cast<char*>(&Token.DoubleValue) + (8 + BytesRead));
            BytesRead += end - begin;
        }

        if (BytesRead == 0) {
            ProduceToken(ETokenType::Double);
        }

        return end;
    }

    const char* ReadAfterPlus(const char* begin, const char* end)
    {
        if (begin == end)
            return begin;

        if (!isdigit(*begin)) {
            ProduceToken(ETokenType::Plus);
            return begin;
        }

        Reset();
        TokenBuffer.push_back('+');
        SetInProgressState(EInnerState::InsideNumeric);
        return ReadNumeric(begin, end);
    }

    void FinishString()
    {
        ProduceToken(ETokenType::String);
    }

    void FinishNumeric()
    {
        try {
            Token.IntegerValue = FromString<i64>(GetBufferAsStringBuf());
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            THROW_ERROR_EXCEPTION("Failed to parse Integer literal %s",
                ~GetBufferAsString().Quote());
        }
        ProduceToken(ETokenType::Integer);
    }

    void FinishDouble()
    {
        try {
            Token.DoubleValue = FromString<double>(GetBufferAsStringBuf());
        } catch (const std::exception& ex) {
            // This exception is wrapped in parser
            THROW_ERROR_EXCEPTION("Failed to parse Double literal %s",
                ~GetBufferAsString().Quote());
        }
        ProduceToken(ETokenType::Double);
    }

    void FinishPlus()
    {
        ProduceToken(ETokenType::Plus);
    }

    void ProduceToken(ETokenType type)
    {
        YASSERT(InnerState != EInnerState::Terminal);
        Token.Type_ = type;
        InnerState = EInnerState::Terminal;
    }

    void SetInProgressState(EInnerState innerState)
    {
        YASSERT(innerState != EInnerState::None);
        InnerState = innerState;
    }

    TStringBuf GetBufferAsStringBuf()
    {
        return TStringBuf(&*TokenBuffer.begin(), TokenBuffer.size());
    }

    Stroka GetBufferAsString()
    {
        return Stroka(&*TokenBuffer.begin(), TokenBuffer.size());
    }

    EInnerState InnerState;
    TToken Token;
    std::vector<char> TokenBuffer;

    /*
     * BytesRead > 0 means we've read BytesRead bytes (in binary integers)
     * BytesRead < 0 means we are expecting -BytesRead bytes more (in binary doubles and strings)
     * BytesRead = 0 also means we don't know the number of bytes yet
     */
    int BytesRead;
};

////////////////////////////////////////////////////////////////////////////////

TLexer::TLexer()
    : Impl(new TLexerImpl())
{ }

TLexer::~TLexer()
{ }

size_t TLexer::Read(const TStringBuf& data)
{
    return Impl->Read(data);
}

void TLexer::Finish()
{
    Impl->Finish();
}

void TLexer::Reset()
{
    Impl->Reset();
}
TLexer::EState TLexer::GetState() const
{
    return Impl->GetState();
}

const TToken& TLexer::GetToken() const
{
    return Impl->GetToken();
}

////////////////////////////////////////////////////////////////////////////////

class TStatelessLexer::TImpl
{
private:
    THolder<TYsonStatelessLexerImplBase> Impl;

public:
    TImpl(bool enableLinePositionInfo = false)
        : Impl(enableLinePositionInfo? 
        static_cast<TYsonStatelessLexerImplBase*>(new TYsonStatelessLexerImpl<true>()) 
        : static_cast<TYsonStatelessLexerImplBase*>(new TYsonStatelessLexerImpl<false>()))
    { }

    size_t GetToken(const TStringBuf& data, TToken* token)
    {
        return Impl->GetToken(data, token);
    }
};

////////////////////////////////////////////////////////////////////////////////

TStatelessLexer::TStatelessLexer()
    : Impl(new TImpl())
{ }

TStatelessLexer::~TStatelessLexer()
{ }

size_t TStatelessLexer::GetToken(const TStringBuf& data, TToken* token)
{
    return Impl->GetToken(data, token);
}

} // namespace NYson
} // namespace NYT
