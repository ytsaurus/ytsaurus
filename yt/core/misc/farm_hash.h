#pragma once

#include <util/system/defaults.h>

#include <contrib/libs/farmhash/farmhash.h>

////////////////////////////////////////////////////////////////////////////////

static inline ui64 FarmHash(ui64 value)
{
    return NFarmHashPrivate::Fingerprint(value);
}

static inline ui64 FarmHash(const void* buf, size_t len)
{
    return NFarmHashPrivate::Hash64(static_cast<const char*>(buf), len);
}

static inline ui64 FarmHash(const void* buf, size_t len, ui64 seed)
{
    return NFarmHashPrivate::Hash64WithSeed(static_cast<const char*>(buf), len, seed);
}

static inline ui64 FarmFingerprint(ui64 value)
{
    return NFarmHashPrivate::Fingerprint(value);
}

static inline ui64 FarmFingerprint(const void* buf, size_t len)
{
    return NFarmHashPrivate::Fingerprint64(static_cast<const char*>(buf), len);
}

static inline ui64 FarmFingerprint(ui64 first, ui64 second)
{
    return NFarmHashPrivate::Fingerprint(NFarmHashPrivate::Uint128(first, second));
}

////////////////////////////////////////////////////////////////////////////////
