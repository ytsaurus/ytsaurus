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

    TOwnerId Owner;

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

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& owner, const TKeyId& key) override;

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

    [[nodiscard]] TOwnerId GetOwner() override;

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) override;

private:
    TCypressKeyWriterConfigPtr Config_;
    NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriter)

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetCypressKeyPath(
    const NYPath::TYPath& prefix,
    const TOwnerId& owner,
    const TKeyId& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
