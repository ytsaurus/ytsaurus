#pragma once

#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/core/nbs/cloud/blockstore/libs/kikimr/events.h>
#include <contrib/ydb/core/nbs/cloud/blockstore/public/api/protos/io.pb.h>
#include <contrib/ydb/library/actors/core/actorid.h>


namespace NYdb::NBS {

    struct TEvService {

        //
        // Events declaration
        //

        enum EEvents
        {
            EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_NBS_V2),

            EvReadBlocksRequest,
            EvReadBlocksResponse,

            EvWriteBlocksRequest,
            EvWriteBlocksResponse,
        };

        BLOCKSTORE_DECLARE_PROTO_EVENTS(WriteBlocks)
        BLOCKSTORE_DECLARE_PROTO_EVENTS(ReadBlocks)
    };

} // namespace NYdb::NBS
