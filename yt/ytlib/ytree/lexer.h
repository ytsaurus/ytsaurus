#pragma once

#include "public.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TLexer
{
public:
    DECLARE_ENUM(EState,
        (None)
        (InProgress)
        // Terminal states:
        (String)
        (Int64)
        (Double)
        (Semicolon) // ;
        (Equals) // =
        (LeftBracket) // [
        (RightBracket) // ]
        (LeftBrace) // {
        (RightBrace) // }
        (LeftAngle) // <
        (RightAngle) // >
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

    const Stroka& GetStringValue() const;
    i64 GetInt64Value() const;
    double GetDoubleValue() const;

    //! Returns special value (in terminal states other than String, Int64 and
    //! Double) as a string. For diagnostics purposes.
    const Stroka& GetSpecialValue() const;
    
private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
