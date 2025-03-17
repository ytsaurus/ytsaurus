#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/protos/filestore_config.pb.h>

namespace NKikimr {

namespace TEvFileStore {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_FILESTORE),

        EvUpdateConfig = EvBegin + 1,
        EvUpdateConfigResponse = EvBegin + 2,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_FILESTORE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_FILESTORE)");

    struct TEvUpdateConfig : TEventPB<TEvUpdateConfig,
            NKikimrFileStore::TUpdateConfig, EvUpdateConfig> {
        TEvUpdateConfig() {}
    };

    struct TEvUpdateConfigResponse : TEventPB<TEvUpdateConfigResponse,
            NKikimrFileStore::TUpdateConfigResponse, EvUpdateConfigResponse> {
        TEvUpdateConfigResponse() {}
    };
};

}
