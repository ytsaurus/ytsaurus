#pragma once

#include "config.h"

#include <yt/yt/flow/library/cpp/common/public.h>

#include <yt/yt/flow/library/cpp/misc/node_info.h>
#include <yt/yt/flow/library/cpp/misc/self_signed_certificate.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/misc/proc.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TNodeInfoPtr GetNodeInfo(const TFlowNodeConfigPtr& config, const NLogging::TLogger& logger);

//! Extracts the snapshot id from the porto container name of the current
//! process ("..._sn_<ID>_start" workload containers of snapshot stages).
//! Returns null outside a snapshot-stage box.
std::optional<std::string> TryExtractDeploySnapshotId(const std::vector<TProcessCgroup>& cgroups);

//! Generates a TLS certificate identifying the node incarnation: the SAN holds the RPC address IP and,
//! when the node name is a host name, the node name as well. Rotating it takes a new incarnation.
TSelfSignedCertificate GenerateIncarnationCertificate(const TNodeInfoBase& nodeInfo);

//! Returns the config the bus server must run with: one serving TLS with a certificate of the node
//! incarnation, which is published in |nodeInfo| so that the runner can pin it in the direct mode.
//! Leaves |busServerConfig| untouched, keeping the generated private key out of the node config, and
//! returns it as is when the config gives the bus server a certificate and key of its own or disables
//! encryption.
NBus::NTcp::TBusServerConfigPtr CreateBusServerConfigWithIncarnationCertificate(
    TNodeInfo* nodeInfo,
    const NBus::NTcp::TBusServerConfigPtr& busServerConfig,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
