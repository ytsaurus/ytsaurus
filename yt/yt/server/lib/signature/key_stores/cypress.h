#pragma once

#include "public.h"

#include <yt/yt/server/lib/signature/key_store.h>

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

static const NYPath::TYPath KeyStorePath("//sys/public_keys/by_owner");

//! Time to wait after key expiration before deleting it from Cypress.
static const auto KeyExpirationMargin = TDuration::Hours(1);

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyReader
    : public IKeyStoreReader
{
public:
    TCypressKeyReader(NApi::IClientPtr client);

    TFuture<TKeyInfoPtr> FindKey(const TOwnerId& owner, const TKeyId& key) override;

private:
    NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyReader)

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyWriter
    : public IKeyStoreWriter
{
public:
    TCypressKeyWriter(
        TOwnerId owner,
        NApi::IClientPtr client);

    [[nodiscard]] TOwnerId GetOwner() override;

    TFuture<void> RegisterKey(const TKeyInfoPtr& keyInfo) override;

private:
    TOwnerId Owner_;
    NApi::IClientPtr Client_;
};

DEFINE_REFCOUNTED_TYPE(TCypressKeyWriter)

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetCypressKeyPath(const TOwnerId& owner, const TKeyId& key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
