#pragma once

#include "public.h"

#include "yt/yt/core/actions/future.h"

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct IKeyStoreReader
    : public virtual TRefCounted
{
    virtual TFuture<TKeyInfoPtr> FindKey(const TOwnerId& owner, const TKeyId& key) = 0;

    virtual ~IKeyStoreReader() = default;
};

DEFINE_REFCOUNTED_TYPE(IKeyStoreReader)

////////////////////////////////////////////////////////////////////////////////

struct IKeyStoreWriter
    : public virtual TRefCounted
{
    [[nodiscard]] virtual TOwnerId GetOwner() = 0;

    virtual TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) = 0;

    virtual ~IKeyStoreWriter() = default;
};

DEFINE_REFCOUNTED_TYPE(IKeyStoreWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
