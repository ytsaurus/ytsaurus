#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

IChangelogStorePtr CreateRemoteChangelogStore(
    TRemoteChangelogStoreConfigPtr config,
    const NYPath::TYPath& remotePath,
    NApi::IClientPtr masterClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
