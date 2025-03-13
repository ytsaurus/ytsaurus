#pragma once

#include <contrib/ydb/library/folder_service/proto/config.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

namespace NKikimr::NFolderService {

NActors::IActor* CreateMockFolderServiceAdapterActor(
        const NKikimrProto::NFolderService::TFolderServiceConfig& config,
        const TMaybe<TString> mockedCloudId = Nothing()
);
}
