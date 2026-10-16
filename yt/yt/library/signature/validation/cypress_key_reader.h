#pragma once

#include "public.h"

#include <yt/yt/library/signature/common/key_store.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

#include <library/cpp/yt/misc/property.h>

#include <utility>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyReader
    : public IKeyStoreReader
{
public:
    TCypressKeyReader(TCypressKeyReaderConfigPtr config, NApi::IClientPtr client);

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& ownerId, const TKeyId& keyId) const final;

    void Reconfigure(TCypressKeyReaderConfigPtr config);

private:
    using TKeyDescriptor = std::pair<TOwnerId, TKeyId>;

    //! A fetched key wrapped for storing in the SLRU cache.
    class TCachedKeyInfo
        : public TAsyncCacheValueBase<TKeyDescriptor, TCachedKeyInfo>
    {
    public:
        DEFINE_BYREF_RO_PROPERTY(TKeyInfoPtr, KeyInfo);

    public:
        TCachedKeyInfo(const TKeyDescriptor& key, TKeyInfoPtr keyInfo);
    };

    using TCachedKeyInfoPtr = TIntrusivePtr<TCachedKeyInfo>;

    //! Caches fetched keys and deduplicates concurrent fetches of the same key.
    //! Fetch errors are not cached.
    class TKeyCache
        : public TAsyncSlruCacheBase<TKeyDescriptor, TCachedKeyInfo>
    {
    public:
        TKeyCache(TCypressKeyReaderConfigPtr config, NApi::IClientPtr client);

        TFuture<TKeyInfoPtr> FindKey(const TOwnerId& ownerId, const TKeyId& keyId);

    private:
        const TCypressKeyReaderConfigPtr Config_;
        const NApi::IClientPtr Client_;
    };

    using TKeyCachePtr = TIntrusivePtr<TKeyCache>;

    const NApi::IClientPtr Client_;

    //! Replaced wholesale on Reconfigure: in-flight fetches complete against
    //! the old cache while all subsequent reads observe the new config.
    TAtomicIntrusivePtr<TKeyCache> KeyCache_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReader)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
