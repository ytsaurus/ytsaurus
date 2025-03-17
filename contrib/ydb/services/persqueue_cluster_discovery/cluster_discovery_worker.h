#pragma once

#include "counters.h"

#include <contrib/ydb/core/grpc_services/grpc_request_proxy.h>
#include <contrib/ydb/core/persqueue/cluster_tracker.h>
#include <contrib/ydb/core/util/address_classifier.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NPQ::NClusterDiscovery::NWorker {

using namespace NActors;
using namespace NAddressClassifier;
using namespace NGRpcService;
using namespace NPQ::NClusterTracker;
using namespace NCounters;

IActor* CreateClusterDiscoveryWorker(TEvDiscoverPQClustersRequest::TPtr& ev,
                                     TLabeledAddressClassifier::TConstPtr datacenterClassifier,
                                     TLabeledAddressClassifier::TConstPtr cloudNetsClassifier,
                                     TClustersList::TConstPtr clustersList,
                                     TClusterDiscoveryCounters::TPtr counters);

} // namespace NKikimr::NClusterDiscovery::NWorker
