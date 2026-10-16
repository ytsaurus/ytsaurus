#pragma once

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

namespace NYT::NFlow::NTesting {

////////////////////////////////////////////////////////////////////////////////

//! Single uint64 "key" column schema, matching MakeKey<ui64>(...). The default key
//! schema for the test harness.
NTableClient::TTableSchemaPtr DefaultTestKeySchema();

//! Builds a validated input message on |streamId| with |key| and dummy timestamps;
//! |init| (if given) fills the payload via builder.Payload().
TInputMessageConstPtr MakeTestMessage(
    const TStreamId& streamId,
    const TKey& key,
    const NTableClient::TTableSchemaPtr& schema,
    const TMessageBuilder::TInitFunction& init = {});

//! Builds a raw (un-keyed) message for source functions.
TMessage MakeTestRawMessage(
    const TStreamId& streamId,
    const NTableClient::TTableSchemaPtr& schema,
    const TMessageBuilder::TInitFunction& init = {});

//! Builds a validated input timer for |key| triggering at |triggerTimestamp|.
TInputTimerConstPtr MakeTestTimer(
    const TKey& key,
    TSystemTimestamp triggerTimestamp,
    const NTableClient::TTableSchemaPtr& keySchema = DefaultTestKeySchema(),
    const TStreamId& streamId = {});

//! Builds a validated input visit for |key|.
TInputVisitConstPtr MakeTestVisit(
    const TKey& key,
    const TStreamId& streamId = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTesting
