#pragma once

#include <util/generic/hash.h>

////////////////////////////////////////////////////////////////////////////////

//! Combines a pair of hash values.
//! Cf. |boost::hash_combine|.
inline size_t HashCombineImpl(size_t first, size_t second)
{
    return first ^ (second + 0x9e3779b9 + (second << 6) + (second >> 2));
}

//! Combines a hash seed with the hash of another value.
template <class T>
inline size_t HashCombine(size_t seed, const T& value)
{
    return HashCombineImpl(seed, THash<T>()(value));
}

//! A hasher for std::pair.
template <class T1, class T2>
struct hash<std::pair<T1, T2>>
{
    size_t operator () (const std::pair<T1, T2>& pair) const
    {
        return THash<T1>()(pair.first) + 1877 * THash<T2>()(pair.second);
    }
};

////////////////////////////////////////////////////////////////////////////////
