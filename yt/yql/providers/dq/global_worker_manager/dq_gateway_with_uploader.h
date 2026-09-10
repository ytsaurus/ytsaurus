#pragma once

#include <contrib/ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

#include <yt/yql/providers/dq/actors/yt/resource_manager.h>

#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_settings.h>

#include <contrib/ydb/library/actors/core/actorsystem.h>

#include <functional>

namespace NYql {

using TUploadClusterResolver = std::function<void(TResourceManagerOptions* opts, const TDqSettings::TPtr& settings)>;

// Creates an IDqGateway proxy that uploads all files referenced in task metas
// to YT before forwarding ExecutePlan to the underlying gateway.
//
// This allows file uploading to happen on the client side (before the gRPC call
// to the vanilla operation's dq_service_process), so the GWM inside the vanilla
// operation only needs to allocate workers, not upload files.
//
// uploadOptions.Files is ignored; files are collected from task metas automatically.
// uploadOptions.UploadPrefix must be set to the YT path prefix for uploads.
TIntrusivePtr<IDqGateway> CreateDqGatewayWithUploader(
    TIntrusivePtr<IDqGateway> underlying,
    NActors::TActorSystem* actorSystem,
    TResourceManagerOptions uploadOptions,
    TIntrusivePtr<ICoordinationHelper> coordinator,
    TUploadClusterResolver resolveUploadCluster = {});

} // namespace NYql
