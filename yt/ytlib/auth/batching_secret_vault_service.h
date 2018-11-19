#pragma once

#include "public.h"

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

ISecretVaultServicePtr CreateBatchingSecretVaultService(
    TBatchingSecretVaultServiceConfigPtr config,
    ISecretVaultServicePtr underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
