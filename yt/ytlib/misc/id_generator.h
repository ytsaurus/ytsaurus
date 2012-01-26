#pragma once

#include "guid.h"

#include <util/digest/murmur.h>


namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Generates a consequent deterministic ids of a given numeric type.
/*! 
 *  When a fresh instance is created, it gets initialized with zero.
 *  Calling #Next produces just the next numeric value.
 *  The generator's state can be serialized by calling overloaded #Save
 *  and #Load.
 *  
 *  Internally, the generator uses an <tt>intptr_t</tt> type to keep the current id value.
 *  Hence the period is equal to the size of <tt>intptr_t</tt>'s domain and may vary
 *  depending on the current bitness.
 *
 *  \note: Thread affinity: Any.
 */
template <class T>
class TIdGenerator
{
public:
    typedef TIdGenerator<T> TThis;

    TIdGenerator()
        : Current(0)
    { }

    T Next()
    {
        return static_cast<T>(AtomicIncrement(Current));
    }

    void Reset()
    {
        Current = 0;
    }

    // Always use a fixed-width type for serialization.
    void Save(TOutputStream* output) const
    {
        ui64 current = static_cast<ui64>(Current);
        ::Save(output, current);
    }

    void Load(TInputStream* input)
    {
        ui64 current;
        ::Load(input, current);
        Current = static_cast<intptr_t>(current);
    }

private:
    TAtomic Current;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT


