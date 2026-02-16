#pragma once

#include "public.h"

#include "key_store.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

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
    TAtomicIntrusivePtr<TCypressKeyReaderConfig> Config_;
    const NApi::IClientPtr Client_;
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
