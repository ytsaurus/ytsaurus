#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct IKeyStoreReader
    : public virtual TRefCounted
{
    virtual TFuture<TKeyInfoPtr> FindKey(const TOwnerId& ownerId, const TKeyId& keyId) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IKeyStoreReader)

////////////////////////////////////////////////////////////////////////////////

struct IKeyStoreWriter
    : public virtual TRefCounted
{
    [[nodiscard]] virtual const TOwnerId& GetOwner() const = 0;

    virtual TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) = 0;
};

DEFINE_REFCOUNTED_TYPE(IKeyStoreWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
