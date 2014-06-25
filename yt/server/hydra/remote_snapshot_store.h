#pragma once

#include "public.h"

#include <ytlib/api/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/election/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    const NYPath::TYPath& remotePath,
    NApi::IClientPtr masterClient);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
