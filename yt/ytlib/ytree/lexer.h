#pragma once

#include "public.h"
#include "token.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TLexerImpl;

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

    //! Consumes a single symbol.
    /*!
     *  Throws an exception if the symbol is unexpected.
     *  
     *  \param ch A symbol to consume.
     *  
     *  \return 
     *  If the symbol is not unexpected but was not consumed (like ';' or ']' 
     *  in "[12; 34]"), the method returns False and the lexer moves to a terminal
     *  state. In this case the client must call #Reset and then #Consume again
     *  with the same symbol once more to proceed.
     */

    //! Reads data range
    /*!
     * Throws an exception if meets an unexpected character.
     *
     *  \param data A range to consume.
     *  \returns Number of consumed characters. If that number is less than
     *  the length of the data range the lexer turns to the terminal state.
     *  In this case the client should call #Reset and then #Read
     *  the remaining suffix to proceed.
     */
    size_t Read(const TStringBuf& data);

    //! Indicates the end-of-stream.
    void Finish();

    void Reset();
    
    //! Returns the current public state.
    EState GetState() const;

    //! Can only be called in a terminal state. Returns the just-parsed token.
    const TToken& GetToken() const;
    
private:
    THolder<TLexerImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
