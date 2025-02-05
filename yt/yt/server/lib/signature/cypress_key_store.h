#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NSignature {

////////////////////////////////////////////////////////////////////////////////

IKeyStoreReaderPtr CreateCypressKeyReader(
    TCypressKeyReaderConfigPtr config,
    NApi::IClientPtr client);

TFuture<IKeyStoreWriterPtr> CreateCypressKeyWriter(
    TCypressKeyWriterConfigPtr config,
    NApi::IClientPtr client);

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath MakeCypressKeyPath(
    const NYPath::TYPath& prefix,
    const TOwnerId& ownerId,
    const TKeyId& keyId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSignature
