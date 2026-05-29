#pragma once
#include <contrib/ydb/core/tx/columnshard/columnshard.h>
#include <contrib/ydb/core/tx/columnshard/data_sharing/protos/events.pb.h>

#include <contrib/ydb/library/actors/core/event_pb.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

struct TEvCheckStatusFromInitiator
    : public NActors::TEventPB<TEvCheckStatusFromInitiator, NKikimrColumnShardDataSharingProto::TEvCheckStatusFromInitiator,
          TEvColumnShard::EvDataSharingCheckStatusFromInitiator> {
    TEvCheckStatusFromInitiator() = default;

    TEvCheckStatusFromInitiator(const TString& sessionId) {
        Record.SetSessionId(sessionId);
    }
};

}   // namespace NKikimr::NOlap::NDataSharing::NEvents
