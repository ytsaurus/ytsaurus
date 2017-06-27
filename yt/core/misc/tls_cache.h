#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*
 * Returns cached value from singleton storage synchronized with spinlock.
 */
template <typename TTrait, typename... U>
typename TTrait::TValue& GetGloballyCachedValue(U&&... u);

/*
 * Returns cached value from thread local storage.
 */
template <typename TTrait, typename... U>
typename TTrait::TValue& GetLocallyCachedValue(U&&... u);

/*
 * Returns cached value from:
 * 1. thread local storage, or
 * 2. synchronized singleton in case of absence in thread local storage.
 */
template <typename TTrait, typename... U>
typename TTrait::TValue& GetLocallyGloballyCachedValue(U&&... u);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define TLS_CACHE_INL_H_
#include "tls_cache-inl.h"
#undef TLS_CACHE_INL_H_
