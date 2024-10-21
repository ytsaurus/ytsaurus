#pragma once

#include "private.h"

#include <yt/yt/server/lib/hive/proto/hive_manager.pb.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct TOutcomingMessage
{
    TSerializedMessagePtr SerializedMessage;
    NTracing::TTraceContextPtr TraceContext;
    TLogicalTime Time;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
};

struct TPersistentMailboxStateCookie
{
    TMessageId NextPersistentIncomingMessageId = 0;
    TMessageId FirstOutcomingMessageId = 0;
    std::vector<TOutcomingMessage> OutcomingMessages;
};

void ToProto(
    NProto::TPersistentMailboxStateCookie* protoCookie,
    const TPersistentMailboxStateCookie& cookie);

void FromProto(
    TPersistentMailboxStateCookie* cookie,
    const NProto::TPersistentMailboxStateCookie& protoCookie);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
