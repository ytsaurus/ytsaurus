#pragma once

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/protos/blockstore_config.pb.h>

namespace NKikimr {

namespace TEvBlockStore {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_BLOCKSTORE) + 1011,

        EvUpdateVolumeConfig = EvBegin + 13,
        EvUpdateVolumeConfigResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_BLOCKSTORE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BLOCKSTORE)");

    struct TEvUpdateVolumeConfig : TEventPB<TEvUpdateVolumeConfig,
            NKikimrBlockStore::TUpdateVolumeConfig, EvUpdateVolumeConfig> {
        TEvUpdateVolumeConfig() {}
    };

    struct TEvUpdateVolumeConfigResponse : TEventPB<TEvUpdateVolumeConfigResponse,
            NKikimrBlockStore::TUpdateVolumeConfigResponse, EvUpdateVolumeConfigResponse> {
        TEvUpdateVolumeConfigResponse() {}
    };
};

}
