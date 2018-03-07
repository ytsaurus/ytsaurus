#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Globally cached value.
/*!
 * Returns a cached value from singleton storage synchronized with spinlock.
 * TTrait must contain
 * - TKey type that represents key for cache lookup.
 * - TValue type representing value.
 * - ToKey static method from U&&... to create key.
 * - ToValue static method from U&&... to create value.
 */
template <typename TTrait, typename... U>
typename TTrait::TValue& GetGloballyCachedValue(U&&... u);

//! Thread local cached value.
/*!
 * Returns a cached value from thread local storage.
 *
 * TTrait is described above for GetGloballyCachedValue function.
 */
template <typename TTrait, typename... U>
typename TTrait::TValue& GetLocallyCachedValue(U&&... u);

//! Thread local cached value together with globally cached value.
/*!
 * Returns a cached value from:
 * 1. thread local storage, or
 * 2. synchronized singleton in case of absence in thread local storage.
 *
 * TTrait is described above for GetGloballyCachedValue function.
 */
template <typename TTrait, typename... U>
typename TTrait::TValue& GetLocallyGloballyCachedValue(U&&... u);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TLS_CACHE_INL_H_
#include "tls_cache-inl.h"
#undef TLS_CACHE_INL_H_
