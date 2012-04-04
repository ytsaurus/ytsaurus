#pragma once

#include "public.h"
#include "token.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TLexer
{
public:
    DECLARE_ENUM(EState,
        (None)
        (InProgress)
        (Terminal)
    );

    TLexer();
    ~TLexer();

    //! Returns true iff the character was consumed.
    /*!
     * Throws yexception if the character is unexpected.
     * If the character is not unexpected but was not consumed (like ';' or ']' 
     * in "[12; 34]"), the method returns false and the lexer comes to terminal
     * state. In this case, user should reset the lexer and call the method
     * with the same character once more to proceed.
     */
    bool Consume(char ch);
    void Finish();
    void Reset();
    
    EState GetState() const;

    //! Returns parsed token when in terminal state.
    const TToken& GetToken() const;
    
private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

TToken ChopToken(const TStringBuf& data, TStringBuf* suffix);
Stroka ChopStringToken(const TStringBuf& data, TStringBuf* suffix);
i64 ChopIntegerToken(const TStringBuf& data, TStringBuf* suffix);
double ChopDoubleToken(const TStringBuf& data, TStringBuf* suffix);
ETokenType ChopSpecialToken(const TStringBuf& data, TStringBuf* suffix);

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
