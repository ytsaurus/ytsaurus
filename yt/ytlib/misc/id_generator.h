#pragma once

#include "guid.h"

#include <util/digest/murmur.h>

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

template <class T>
class TIdGenerator;

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Save(TOutputStream* output, const NYT::TIdGenerator<T>& generator);

template <class T>
void Load(TInputStream* input, NYT::TIdGenerator<T>& generator);

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

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

private:
    TAtomic Current;

    friend void ::Save<>(TOutputStream* output, const TThis& generator);
    friend void ::Load<>(TInputStream* input, TThis& generator);
};

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

// Always use a fixed-width type for serialization.

template <class T>
void Save(TOutputStream* output, const NYT::TIdGenerator<T>& generator)
{
    ui64 current = static_cast<ui64>(generator.Current);
    Save(output, current);
}

template <class T>
void Load(TInputStream* input, NYT::TIdGenerator<T>& generator)
{
    ui64 current;
    Load(input, current);
    generator.Current = static_cast<intptr_t>(current);
}

////////////////////////////////////////////////////////////////////////////////
