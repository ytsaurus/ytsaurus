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

bool IsEmpty(const TStringBuf& data);
TToken ChopToken(const TStringBuf& data, Stroka* suffix = NULL);
Stroka ChopStringToken(const TStringBuf& data, Stroka* suffix = NULL);

i64 ChopIntegerToken(const TStringBuf& data, Stroka* suffix);
double ChopDoubleToken(const TStringBuf& data, Stroka* suffix);
ETokenType ChopSpecialToken(const TStringBuf& data, Stroka* suffix);

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
