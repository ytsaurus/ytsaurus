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
    //         BinaryScalar                =  xx01b
    //             BinaryInt64             =  0001b
    //             BinaryDouble            =  0101b
    //             BinaryFalse             =  1001b
    //             BinaryTrue              =  1101b
    //         Other                       = xxx11b
    //             Quote                   = 00011b
    //             DigitOrMinus            = 00111b
    //             String                  = 01011b
    //             Space                   = 01111b
    //             Plus                    = 10011b
    //             None                    = 10111b
    //             Percent                 = 11011b
    DECLARE_ENUM(EReadStartCase,
        ((BinaryString)                 (0))    // =    00b
        ((OtherSpecialToken)            (2))    // =    10b

        ((BinaryInt64)                  (1))    // =   001b
        ((BinaryDouble)                 (5))    // =   101b
        ((BinaryFalse)                  (9))    // =  1001b
        ((BinaryTrue)                   (13))   // =  1101b
        ((BinaryUint64)                 (17))   // = 10001b

        ((Quote)                        (3))    // = 00011b
        ((DigitOrMinus)                 (7))    // = 00111b
        ((String)                       (11))   // = 01011b
        ((Space)                        (15))   // = 01111b
        ((Plus)                         (19))   // = 10011b
        ((None)                         (23))   // = 10111b
        ((Percent)                      (27))   // = 11011b
    );

    static EReadStartCase GetStartState(char ch)
    {
#define NN EReadStartCase::None
#define BS EReadStartCase::BinaryString
#define BI EReadStartCase::BinaryInt64
#define BD EReadStartCase::BinaryDouble
#define BF EReadStartCase::BinaryFalse
#define BT EReadStartCase::BinaryTrue
#define BU EReadStartCase::BinaryUint64
        //EReadStartCase::Space
#define SP NN
#define DM EReadStartCase::DigitOrMinus
#define ST EReadStartCase::String
#define PL EReadStartCase::Plus
#define QU EReadStartCase::Quote
#define PC EReadStartCase::Percent

        static const ui8 lookupTable[] =
        {
            NN,BS,BI,BD,BF,BT,BU,NN,NN,SP,SP,SP,SP,SP,NN,NN,
            NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,NN,

            // 32
            SP, // ' '
            NN, // '!'
            QU, // '"'
            ETokenType::Hash << 2 | EReadStartCase::OtherSpecialToken, // '#'
            NN, // '$'
            PC, // '%'
            NN, // '&'
            NN, // "'"
            ETokenType::LeftParenthesis << 2 | EReadStartCase::OtherSpecialToken, // '('
            ETokenType::RightParenthesis << 2 | EReadStartCase::OtherSpecialToken, // ')'
            NN, // '*'
            PL, // '+'
            ETokenType::Comma << 2 | EReadStartCase::OtherSpecialToken, // ','
            DM, // '-'
            NN, // '.'
            NN, // '/'

            // 48
            DM,DM,DM,DM,DM,DM,DM,DM,DM,DM, // '0' - '9'
            ETokenType::Colon << 2 | EReadStartCase::OtherSpecialToken, // ':'
            ETokenType::Semicolon << 2 | EReadStartCase::OtherSpecialToken, // ';'
            ETokenType::LeftAngle << 2 | EReadStartCase::OtherSpecialToken, // '<'
            ETokenType::Equals << 2 | EReadStartCase::OtherSpecialToken, // '='
            ETokenType::RightAngle << 2 | EReadStartCase::OtherSpecialToken, // '>'
            NN, // '?'

            // 64
            NN, // '@'
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'A' - 'M'
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'N' - 'Z'
            ETokenType::LeftBracket << 2 | EReadStartCase::OtherSpecialToken, // '['
            NN, // '\'
            ETokenType::RightBracket << 2 | EReadStartCase::OtherSpecialToken, // ']'
            NN, // '^'
            ST, // '_'

            // 96
            NN, // '`'
            
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'a' - 'm'
            ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST,ST, // 'n' - 'z'
            ETokenType::LeftBrace << 2 | EReadStartCase::OtherSpecialToken, // '{'
            NN, // '|'
            ETokenType::RightBrace << 2 | EReadStartCase::OtherSpecialToken, // '}'
            NN, // '~'
            NN, // '^?' non-printable
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
                } else if (state == EReadStartCase::Percent) {
                    TBase::Advance(1);
                    *token = TToken(TBase::template ReadBoolean<true>());
                } else { // None
                    YASSERT(state == EReadStartCase::None);
                    THROW_ERROR_EXCEPTION("Unexpected %Qv",
                        ch);
                }
            } else { // BinaryScalar = x01b
                TBase::Advance(1);
                if (state == EReadStartCase::BinaryDouble) {
                    double value;
                    TBase::ReadBinaryDouble(&value);
                    *token = TToken(value);
                } else if (state == EReadStartCase::BinaryInt64) {
                    i64 value;
                    TBase::ReadBinaryInt64(&value);
                    *token = TToken(value);
                } else if (state == EReadStartCase::BinaryUint64) {
                    ui64 value;
                    TBase::ReadBinaryUint64(&value);
                    *token = TToken(value);
                } else if (state == EReadStartCase::BinaryFalse) {
                    *token = TToken(false);
                } else if (state == EReadStartCase::BinaryTrue) {
                    *token = TToken(true);
                } else {
                    YUNREACHABLE();
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

    typedef typename TBase::ENumericResult ENumericResult;

    template <bool AllowFinish>
    void ReadNumeric(TToken* token)
    {
        TStringBuf valueBuffer;
        ENumericResult numericResult = TBase::template ReadNumeric<AllowFinish>(&valueBuffer);

        if (numericResult == ENumericResult::Double) {
            try {
                *token = TToken(FromString<double>(valueBuffer));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing double literal %Qv",
                    valueBuffer)
                    << *this
                    << ex;
            }
        } else if (numericResult == ENumericResult::Int64) {
            try {
                *token = TToken(FromString<i64>(valueBuffer));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing int64 literal %Qv",
                    valueBuffer)
                    << *this
                    << ex;
            }
        } else if (numericResult == ENumericResult::Uint64) {
            try {
                *token = TToken(FromString<ui64>(valueBuffer.SubStr(0, valueBuffer.size() - 1)));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error parsing uint64 literal %Qv",
                    valueBuffer)
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
