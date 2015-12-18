#pragma once

#include "public.h"

#include <yt/contrib/farmhash/farmhash.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static inline TFingerprint FarmHash(ui64 value)
{
    return NFarmHashPrivate::Fingerprint(value);
}

static inline TFingerprint FarmHash(const void* buf, size_t len)
{
    return NFarmHashPrivate::Hash64(static_cast<const char*>(buf), len);
}

static inline TFingerprint FarmHash(const void* buf, size_t len, ui64 seed)
{
    return NFarmHashPrivate::Hash64WithSeed(static_cast<const char*>(buf), len, seed);
}

static inline TFingerprint FarmFingerprint(ui64 value)
{
    return NFarmHashPrivate::Fingerprint(value);
}

static inline TFingerprint FarmFingerprint(const void* buf, size_t len)
{
    return NFarmHashPrivate::Fingerprint64(static_cast<const char*>(buf), len);
}

static inline TFingerprint FarmFingerprint(ui64 first, ui64 second)
{
    return NFarmHashPrivate::Fingerprint(NFarmHashPrivate::Uint128(first, second));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
