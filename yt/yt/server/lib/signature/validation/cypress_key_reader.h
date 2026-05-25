#pragma once

#include "public.h"

#include <yt/yt/server/lib/signature/common/key_store.h>

#include <yt/yt/client/api/public.h>

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

} // namespace NYT::NSignature
