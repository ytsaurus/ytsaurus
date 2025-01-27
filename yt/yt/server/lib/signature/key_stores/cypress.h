#pragma once

#include "public.h"

#include <yt/yt/server/lib/signature/key_store.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyReaderConfig
    : public NYTree::TYsonStruct
{
    //! Prefix path for public keys (will be read from <Path>/<OwnerId>/<KeyId>).
    NYPath::TYPath Path;

    REGISTER_YSON_STRUCT(TCypressKeyReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCypressKeyWriterConfig
    : public NYTree::TYsonStruct
{
    //! Prefix path for public keys (will be stored as <Path>/<Owner>/<KeyId>).
    NYPath::TYPath Path;

    TOwnerId OwnerId;

    //! Time to wait after expiration before deleting keys from Cypress.
    TDuration KeyDeletionDelay;

    //! Maximum key count allowed.
    std::optional<int> MaxKeyCount;

    REGISTER_YSON_STRUCT(TCypressKeyWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyReader
    : public IKeyStoreReader
{
public:
    TCypressKeyReader(
        TCypressKeyReaderConfigPtr config,
        NApi::IClientPtr client);

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& ownerId, const TKeyId& keyId) override;

private:
    TCypressKeyReaderConfigPtr Config_;
    NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReader)

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyWriter
    : public IKeyStoreWriter
{
public:
    TCypressKeyWriter(
        TCypressKeyWriterConfigPtr config,
        NApi::IClientPtr client);

    //! Initialize() should be called at least once before all other calls.
    TFuture<void> Initialize();

    const TOwnerId& GetOwner() override;

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) override;

private:
    const TCypressKeyWriterConfigPtr Config_;
    const NApi::IClientPtr Client_;

    TFuture<void> Initialization_;

    TFuture<void> DoRegister(const TKeyInfoPtr& keyInfo);
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriter)

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath MakeCypressKeyPath(
    const NYPath::TYPath& prefix,
    const TOwnerId& ownerId,
    const TKeyId& keyId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
