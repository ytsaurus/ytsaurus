#pragma once

#include "public.h"

#include "key_pair.h"

#include "yt/yt/core/actions/future.h"

namespace NYT::NSignatureService {

// TODO(pavook) futurize.

////////////////////////////////////////////////////////////////////////////////

struct IKeyStoreReader
{
    [[nodiscard]] virtual TKeyInfoPtr GetKey(const TOwnerId& owner, const TKeyId& id) = 0;

    virtual ~IKeyStoreReader() = default;
};

////////////////////////////////////////////////////////////////////////////////

struct IKeyStoreWriter
{
    [[nodiscard]] virtual TOwnerId GetOwner() = 0;

    virtual bool RegisterKey(const TKeyInfo& key) = 0;

    virtual ~IKeyStoreWriter() = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignatureService
