#pragma once

#include <core/misc/public.h>

#include <ytlib/hydra/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCellDirectory)
DECLARE_REFCOUNTED_CLASS(TClusterDirectory)

DECLARE_REFCOUNTED_CLASS(TCellDirectoryConfig)

////////////////////////////////////////////////////////////////////////////////

using NHydra::TCellId;
using NHydra::NullCellId;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
