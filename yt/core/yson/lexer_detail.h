#pragma once

#include "detail.h"
#include "token.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

template <class TBlockStream, bool EnableLinePositionInfo>
class TLexer
    : public TLexerBase<TBlockStream, EnableLinePositionInfo>
{
private:
    typedef TLexerBase<TBlockStream, EnableLinePositionInfo> TBase;

    // EReadStartCase tree representation:
    // Root                                =     xb
    //     BinaryStringOrOtherSpecialToken =    x0b
    //         BinaryString                =    00b
    //         OtherSpecialToken           =    10b
    //     Other                           =    x1b
    //         BinaryIntegerOrBinaryDouble =   x01b
    //             BinaryInt64             =   001b
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

        ((BinaryInt64)                  (1))    // =   001b
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
#define BI EReadStartCase::BinaryInt64
#define BD EReadStartCase::BinaryDouble
#define SP NN 
        //EReadStartCase::Space
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

public:
    TLexer(const TBlockStream& blockStream, TNullable<i64> memoryLimit) 
        : TBase(blockStream, memoryLimit)
    { }

    void GetToken(TToken* token)
    {
        char ch = TBase::SkipSpaceAndGetChar();
        auto state = GetStartState(ch);

        if (ch == '\0') {
            *token = TToken::EndOfStream;
            return;
        }

        // EReadStartCase tree representation:
        // Root                                =     xb
        //     BinaryStringOrOtherSpecialToken =    x0b
        //         BinaryString                =    00b
        //         OtherSpecialToken           =    10b
        //     Other                           =    x1b
        //         BinaryIntegerOrBinaryDouble =   x01b
        //             BinaryInt64           =   001b
        //             BinaryDouble            =   101b
        //         Other                       = xxx11b
        //             Quote                   = 00011b
        //             DigitOrMinus            = 00111b *
        //             String                  = 01011b
        //             Space                   = 01111b *
        //             Plus                    = 10011b *
        //             None                    = 10111b
        if (state & 1) { // Other = x1b
            if (state & 1 << 1) { // Other = xxx11b
                if (state == EReadStartCase::Quote) {
                    TStringBuf value;
                    TBase::Advance(1);
                    TBase::ReadQuotedString(&value);
                    *token = TToken(value);
                } else if (state == EReadStartCase::DigitOrMinus) {
                    ReadNumeric<true>(token);
                } else if (state == EReadStartCase::Plus) {
                    TBase::Advance(1);

                    char ch = TBase::template GetChar<true>();

                    if (!isdigit(ch)) {
                        *token = TToken(ETokenType(ETokenType::Plus));
                    } else {
                        ReadNumeric<true>(token);
                    }
                } else if (state == EReadStartCase::String) {
                    TStringBuf value;
                    TBase::template ReadUnquotedString<true>(&value);
                    *token = TToken(value);
                } else { // None
                    YASSERT(state == EReadStartCase::None);
                    THROW_ERROR_EXCEPTION("Unexpected %s",
                        ~Stroka(ch).Quote());
                }
            } else { // BinaryIntegerOrBinaryDouble = x01b
                TBase::Advance(1);
                if (state & 1 << 2) { // BinaryDouble = 101b
                    YASSERT(state == EReadStartCase::BinaryDouble);
                    double value;
                    TBase::ReadBinaryDouble(&value);
                    *token = TToken(value);
                } else { // BinaryInt64 = 001b
                    YASSERT(state == EReadStartCase::BinaryInt64);
                    i64 value;
                    TBase::ReadBinaryInt64(&value);
                    *token = TToken(value);
                }
            }
        } else { // BinaryStringOrOtherSpecialToken = x0b
            TBase::Advance(1);
            if (state & 1 << 1) { // OtherSpecialToken = 10b
                YASSERT((state & 3) == EReadStartCase::OtherSpecialToken);
                *token = TToken(ETokenType(state >> 2));
            } else { // BinaryString = 00b
                YASSERT((state & 3) == EReadStartCase::BinaryString);
                TStringBuf value;
                TBase::ReadBinaryString(&value);
                *token = TToken(value);
            }
        }
    }

    template <bool AllowFinish>
    void ReadNumeric(TToken* token)
    {
        TStringBuf valueBuffer;
        bool isDouble = TBase::template ReadNumeric<AllowFinish>(&valueBuffer);

        if (isDouble) {
            try {
                *token = TToken(FromString<double>(valueBuffer));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing double literal %s",
                    ~Stroka(valueBuffer).Quote())
                    << *this
                    << ex;
            }
        } else {
            try {
                *token = TToken(FromString<i64>(valueBuffer));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing integer literal %s",
                    ~Stroka(valueBuffer).Quote())
                    << *this
                    << ex;
            }
        }
    } 
};
////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail

class TStatelessYsonLexerImplBase
{
public:
    virtual size_t GetToken(const TStringBuf& data, TToken* token) = 0;

    virtual ~TStatelessYsonLexerImplBase()
    { }
};

template <bool EnableLinePositionInfo>
class TStatelesYsonLexerImpl : public TStatelessYsonLexerImplBase
{
private:
    typedef NDetail::TLexer<TStringReader, EnableLinePositionInfo> TLexer;
    TLexer Lexer;

public:
    TStatelesYsonLexerImpl()
        : Lexer(TStringReader(), Null)
    { }
    
    size_t GetToken(const TStringBuf& data, TToken* token) override
    {
        Lexer.SetBuffer(data.begin(), data.end());
        Lexer.GetToken(token);
        return Lexer.Begin() - data.begin();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
