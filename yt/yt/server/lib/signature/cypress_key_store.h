#pragma once

#include "public.h"

#include "key_store.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyReader
    : public IKeyStoreReader
{
public:
    TCypressKeyReader(TCypressKeyReaderConfigPtr config, NApi::IClientPtr client);

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& ownerId, const TKeyId& keyId) const final;

    void Reconfigure(const TCypressKeyReaderConfigPtr& config);

private:
    TCypressKeyReaderConfigPtr Config_;
    const NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReader)

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyWriter
    : public IKeyStoreWriter
{
public:
    TCypressKeyWriter(TCypressKeyWriterConfigPtr config, NApi::IClientPtr client);

    //! Initialize() should be called before all other calls.
    TFuture<void> Initialize();

    const TOwnerId& GetOwner() const final;

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) final;

    TFuture<void> Reconfigure(const TCypressKeyWriterConfigPtr& config);

private:
    const TCypressKeyWriterConfigPtr Config_;
    const NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriter)

////////////////////////////////////////////////////////////////////////////////

TFuture<TCypressKeyWriterPtr> CreateCypressKeyWriter(
    TCypressKeyWriterConfigPtr config,
    NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
