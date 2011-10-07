#pragma once

#include "guid.h"

#include <util/digest/murmur.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Generates a consequent deterministic ids of a given numeric type.
/*! 
 *  When a fresh instance is created, it gets initialized with zero.
 *  Calling #Next produces just the next numeric value.
 *  The generator's state can be serialized by calling overloaded <tt> operator &lt;&lt; </tt>
 *  and <tt> operator &gt;&gt; </tt>.
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
    TIdGenerator()
        : Current(0)
    { }

    T Next()
    {
        return static_cast<T>(AtomicIncrement(Current));
    }

private:
    TAtomic Current;

    friend TOutputStream& operator << (TOutputStream& stream, const TIdGenerator& generator);
    friend TInputStream& operator >> (TInputStream& stream, TIdGenerator& generator);

};

////////////////////////////////////////////////////////////////////////////////

//! A specialization for TGuid type.
/*!
 *  The implementation keeps an auto-incrementing <tt>ui64</tt> counter in
 *  the lower part of the TGuid and some hash in the upper part.
 */
template <>
class TIdGenerator<TGuid>
{
public:
    TIdGenerator()
        : Current(0)
    { }

    TGuid Next()
    {
        ui64 counter = static_cast<ui64>(AtomicIncrement(Current));
        ui64 hash = MurmurHash<ui64>(&counter, sizeof (counter));
        return TGuid(
            counter & 0xffffffff,
            counter >> 32,
            hash & 0xffffffff,
            hash >> 32);
    }

private:
    TAtomic Current;

    //template <class T>
    //friend TOutputStream& operator << (TOutputStream& stream, const TIdGenerator<T>& generator);

    //template <class T>
    //friend TInputStream& operator >> (TInputStream& stream, TIdGenerator<T>& generator);

};

////////////////////////////////////////////////////////////////////////////////

// Always use a fixed-width type for serialization.

//template <class T>
//inline TOutputStream& operator << (TOutputStream& stream, const NYT::TIdGenerator<T>& generator)
//{
//    ui64 current = static_cast<ui64>(generator.Current);
//    stream << current;
//    return stream;
//}
//
//template <class T>
//inline TInputStream& operator >> (TInputStream& stream, NYT::TIdGenerator<T>& generator)
//{
//    ui64 current;
//    stream >> current;
//    generator.Current = static_cast<intptr_t>(current);
//    return stream;
//}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT



