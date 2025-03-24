#pragma once

#include "defs.h"

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/interconnect/interconnect.h>
#include <contrib/ydb/core/base/domain.h>
#include <contrib/ydb/core/protos/node_broker.pb.h>

namespace NKikimrConfig {
    class TStaticNameserviceConfig;
} // NKikimrConfig

namespace NKikimrBlobStorage {
    class TStorageConfig;
} // NKikimrBlobStorage

namespace NKikimr {
namespace NNodeBroker {

// Create nameservice for static node.
IActor *CreateDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup,
                                ui32 poolId = 0);

// Create nameservice for dynamic node providing its info.
IActor *CreateDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup> &setup,
                                const NKikimrNodeBroker::TNodeInfo &node,
                                const TDomainsInfo &domains,
                                ui32 poolId = 0);

TIntrusivePtr<TTableNameserverSetup> BuildNameserverTable(const NKikimrConfig::TStaticNameserviceConfig& nsConfig);
TIntrusivePtr<TTableNameserverSetup> BuildNameserverTable(const NKikimrBlobStorage::TStorageConfig& config);

} // NNodeBroker
} // NKikimr
