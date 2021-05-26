#pragma once

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/misc/intrusive_ptr.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TChaosNodeConfig)
DECLARE_REFCOUNTED_CLASS(TChaosManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)

using NTransactionClient::TTransactionSignature;
using NTransactionClient::InitialTransactionSignature;
using NTransactionClient::FinalTransactionSignature;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
