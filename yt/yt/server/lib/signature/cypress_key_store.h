#include "key_store.h"

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

    TFuture<TKeyInfoPtr> GetKey(const TOwnerId& owner, const TKeyId& keyId) override;

private:
    NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

class TCypressKeyWriter
    : public IKeyStoreWriter
{
public:
    TCypressKeyWriter(
        TOwnerId owner,
        NApi::IClientPtr client);

    [[nodiscard]] TOwnerId GetOwner() override;

    TFuture<void> RegisterKey(const TKeyInfo& key) override;

private:
    TOwnerId Owner_;
    NApi::IClientPtr Client_;
};

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath GetCypressKeyPath(const TOwnerId& owner, const TKeyId& keyId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
