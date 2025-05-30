#pragma once

#include "events.h"
#include "private_client.h"

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/event_local.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <contrib/ydb/core/fq/libs/events/event_subspace.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <contrib/ydb/core/fq/libs/shared_resources/shared_resources.h>

#include <contrib/ydb/core/fq/libs/control_plane_storage/proto/yq_internal.pb.h>

namespace NFq {

NActors::IActor* CreateInternalServiceActor(
    const NFq::TYqSharedResources::TPtr& yqSharedResources,
    const NKikimr::TYdbCredentialsProviderFactory& credentialsProviderFactory,
    const NFq::NConfig::TPrivateApiConfig& privateApiConfig,
    const ::NMonitoring::TDynamicCounterPtr& counters);

} /* NFq */
