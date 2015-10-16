#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

IChangelogStoreFactoryPtr CreateRemoteChangelogStoreFactory(
    TRemoteChangelogStoreConfigPtr config,
    TRemoteChangelogStoreOptionsPtr options,
    const NYPath::TYPath& path,
    NApi::IClientPtr masterClient,
    const NTransactionClient::TTransactionId& prerequisiteTransactionId =
        NTransactionClient::NullTransactionId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
