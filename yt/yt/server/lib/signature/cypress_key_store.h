#pragma once

#include "public.h"

#include "key_store.h"

#include <yt/yt/ytlib/api/native/public.h>

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

class TCypressKeyWriter
    : public IKeyStoreWriter
{
public:
    TCypressKeyWriter(TCypressKeyWriterConfigPtr config, TOwnerId ownerId, NApi::NNative::IClientPtr client);

    TOwnerId GetOwner() const final;

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) final;

    void Reconfigure(TCypressKeyWriterConfigPtr config);

private:
    TAtomicIntrusivePtr<TCypressKeyWriterConfig> Config_;
    const TOwnerId OwnerId_;
    const NApi::NNative::IClientPtr Client_;

    TFuture<void> CleanUpKeysIfLimitReached(TCypressKeyWriterConfigPtr config);

    TFuture<void> DoCleanUpOnLimitReached(const TCypressKeyWriterConfigPtr& config, const TErrorOr<NYson::TYsonString>& ownerNode);

    TFuture<void> DoRegisterKey(const TCypressKeyWriterConfigPtr& config, TKeyInfoPtr keyInfo, TOwnerId ownerId, TKeyId keyId);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
