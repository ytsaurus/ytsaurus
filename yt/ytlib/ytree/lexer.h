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
    bool Consume(char ch);

    //! Similar to the other overload but consumes symbols in a batch.
    /*!
     *  \param data A range to consume.
     *  \returns the pointer to the first unconsumed symbol.
     */
    size_t Consume(const TStringBuf& data);

    //! Indicates the end-of-stream.
    void Finish();

    void Reset();
    
    //! Returns the current public state.
    EState GetState() const;

    //! Can only be called in a terminal state. Returns the just-parsed token.
    const TToken& GetToken() const;
    
private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
