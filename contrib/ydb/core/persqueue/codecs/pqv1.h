#pragma once

#include <contrib/ydb/public/api/protos/draft/persqueue_common.pb.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_topic/topic.h>

namespace NKikimr::NPQ {

Ydb::PersQueue::V1::Codec ToV1Codec(const NPersQueueCommon::ECodec codec);
std::optional<NPersQueueCommon::ECodec> FromV1Codec(const NYdb::NPersQueue::ECodec codec);

i32 FromTopicCodec(const NYdb::NTopic::ECodec codec);

} // NKikimr::NPQ
