#include "stdafx.h"
#include "lexer.h"

#include "token.h"

#include <ytlib/misc/zigzag.h>
#include <ytlib/misc/property.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TLexerImpl
{
public:
    DECLARE_ENUM(EInnerState,
        (None)
        (InsideBinaryInteger)
        (InsideBinaryDouble)
        (InsideBinaryString)
        (InsideUnquotedString)
        (InsideQuotedString)
        (InsideNumeric)
        (InsideDouble)
        (AfterPlus)
    );

    TLexerImpl()
    {
        Reset();
        TokenBuffer.reserve(StringBufferSize);
    }

    void Reset()
    {
        State_ = TLexer::EState::None;
        InnerState = EInnerState::None;
        Token = TToken();
        TokenBuffer.clear();
        BytesRead = 0;
    }

    const TToken& GetToken() const
    {
        YASSERT(State_ == TLexer::EState::Terminal);
        return Token;
    }
    
    size_t Read(const TStringBuf& input)
    {
        auto begin = input.begin();
        auto end = input.end();
        switch (State_) {
            case TLexer::EState::None:
                return ReadStart(begin, end) - begin;

            case TLexer::EState::InProgress:
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
                        YUNREACHABLE();
                }

            default:
                // Should not consume chars in terminal states
                YUNREACHABLE();
        }
    }

    void Finish()
    {
        switch (State_) {
            case TLexer::EState::InProgress:
                switch (InnerState) {
                    case EInnerState::InsideBinaryInteger:
                    case EInnerState::InsideBinaryDouble:
                    case EInnerState::InsideBinaryString:
                    case EInnerState::InsideQuotedString:
                        ythrow yexception() << Sprintf("Premature end of stream (LexerState: %s, BytesRead: %d)",
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
                        YUNREACHABLE();
                }
                break;

            default:
                break;
        }
        
    }

    DEFINE_BYVAL_RO_PROPERTY(TLexer::EState, State)

private:
    static const int StringBufferSize = 1 << 16;

    const char* ReadStart(const char* begin, const char* end)
    {
        const char* current = begin;
        while (current != end && isspace(*current)) {
            ++current;
        }
        if (current == end) {
            return current;
        }

        char ch = *current;
        ++current;
        switch (ch) {
            case '"':
                SetInProgressState(EInnerState::InsideQuotedString);
                YASSERT(TokenBuffer.empty());
                YASSERT(BytesRead == 0);
                return ReadQuotedString(current, end);

            case '\x01':
                SetInProgressState(EInnerState::InsideBinaryString);
                YASSERT(TokenBuffer.empty());
                YASSERT(BytesRead == 0);
                return ReadBinaryString(current, end);

            case '\x02':
                SetInProgressState(EInnerState::InsideBinaryInteger);
                YASSERT(Token.IntegerValue == 0);
                YASSERT(BytesRead == 0);
                return ReadBinaryInteger(current, end);

            case '\x03':
                SetInProgressState(EInnerState::InsideBinaryDouble);
                YASSERT(Token.DoubleValue == 0.0);
                YASSERT(BytesRead == 0);
                BytesRead = -static_cast<int>(sizeof(double));
                return ReadBinaryDouble(current, end);

            case '+':
                SetInProgressState(EInnerState::AfterPlus);
                return ReadAfterPlus(current, end);

            default: {
                auto specialTokenType = CharToTokenType(ch);
                if (specialTokenType != ETokenType::EndOfStream) {
                    ProduceToken(specialTokenType);
                    return current;
                } else if (isdigit(ch) || ch == '-') { // case of '+' is handled in AfterPlus state
                    YASSERT(TokenBuffer.empty());
                    TokenBuffer.push_back(ch);
                    SetInProgressState(EInnerState::InsideNumeric);
                    return ReadNumeric(current, end);
                } else if (isalpha(ch) || ch == '_' || ch == '%') {
                    YASSERT(TokenBuffer.empty());
                    TokenBuffer.push_back(ch);
                    SetInProgressState(EInnerState::InsideUnquotedString);
                    return ReadUnquotedString(current, end);
                } else {
                    ythrow yexception() << Sprintf("Unexpected character %s",
                        ~Stroka(ch).Quote());
                }
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

    const char* ReadBinaryInteger(const char* begin, const char* end) {
        ui64 ui64Value = static_cast<ui64>(Token.IntegerValue);
        for (auto current = begin; current != end; ++current) {
            ui8 byte = static_cast<ui8>(*current);

            if (7 * BytesRead > 8 * sizeof(ui64) ) {
                ythrow yexception() << Sprintf("The data is too long to read binary Integer");
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
            if (State_ == TLexer::EState::Terminal) {
                i64 length = Token.IntegerValue;
                if (length < 0) {
                    ythrow yexception() << Sprintf("Error reading binary string: String cannot have negative length (Length: %" PRId64 ")",
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
                Token.StringValue = GetBufferAsStringBuf();
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
                ythrow yexception() << Sprintf("Unexpected character in numeric (Char: %s, Token: %s)",
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
                ythrow yexception() << Sprintf("Unexpected character in numeric (Char: %s, Token: %s)",
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
            ythrow yexception() << Sprintf("Failed to parse Integer literal %s",
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
            ythrow yexception() << Sprintf("Failed to parse Double literal %s",
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
        YASSERT(State_ != TLexer::EState::Terminal);
        InnerState = EInnerState::None;
        Token.Type_ = type;
        State_ = TLexer::EState::Terminal;
    }

    void SetInProgressState(EInnerState innerState)
    {
        YASSERT(innerState != EInnerState::None);
        State_ = TLexer::EState::InProgress;
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

} // namespace NYTree
} // namespace NYT
