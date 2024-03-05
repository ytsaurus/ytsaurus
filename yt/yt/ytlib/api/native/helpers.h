#include "public.h"

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

//! Creates an authenticator which is used for all the internal RPC services.
//!
//! This authenticator uses connection to verify if the ticket source is valid. Now,
//! it's considered valid if it's either from the same cluster or from some cluster
//! specified in the connection's cluster directory.
NRpc::IAuthenticatorPtr CreateNativeAuthenticator(const IConnectionPtr& connection);

////////////////////////////////////////////////////////////////////////////////

//! Setups cluster connection dynamic config update according to a given policy.
/*!
 *  - FromStaticConfig
 *  Deserialize dynamic cluster connection config from static config.
 *  Legacy behaviour.
 *
 *  - FromDynamicConfigWithStaticPatch
 *  Initially, construct cluster connection from static config, and then update
 *  its dynamic part whenever cluster directory notices connection config update.
 *  Let dynamic config be a combination of Cypress config + static config as a patch over it.
 *  Compatible behaviour for a transition period, while some important patches are still
 *  in the static config.
 *
 *  - FromDynamicConfig
 *  Initially, construct cluster connection from static config, and then update
 *  its dynamic part whenever cluster directory notices connection config update.
 *
 *  NB: in two latter modes there is a period of time such that cluster connection
 *  works with statically generated dynamic config. Therefore, you must be careful with
 *  "counter-fuckup" flags, you may not rely on dynamic config taking strictly the
 *  values from the Cypress at any moment of time. E.g. if corrupt_data = %true in
 *  the static config or by default, and corrupt_data = %false in the dynamic config,
 *  then there definitely will be a moment of time, when corrupt_data is true.
 *
 *  If you wish to reliably control dynamic connection via Cypress config,
 *  implement another mode which fetches dynamic config (directly from master)
 *  before creating cluster connection. For now there are no situations when this
 *  is needed.
 */

void SetupClusterConnectionDynamicConfigUpdate(
    const IConnectionPtr& connection,
    EClusterConnectionDynamicConfigPolicy policy,
    const NYTree::INodePtr& staticClusterConnectionNode,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

TFuture<NScheduler::TAllocationBriefInfo> GetAllocationBriefInfo(
    const NScheduler::TOperationServiceProxy& operationServiceProxy,
    NScheduler::TAllocationId allocationId,
    NScheduler::TAllocationInfoToRequest allocationInfoToRequest);

////////////////////////////////////////////////////////////////////////////////

bool IsRevivalError(const TError& error);

TError MakeRevivalError(
    NScheduler::TOperationId operationId,
    NScheduler::TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
