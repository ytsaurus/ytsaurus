#pragma once

#include "private.h"

#include <yt/yt/server/lib/hive/proto/hive_manager.pb.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

struct TPersistentMailboxState
{
    //! The id of the first message in |OutcomingMessages|.
    DEFINE_BYVAL_RW_PROPERTY(TMessageId, FirstOutcomingMessageId);

    struct TOutcomingMessage
    {
        TSerializedMessagePtr SerializedMessage;
        NTracing::TTraceContextPtr TraceContext;
        TLogicalTime Time;

        void Save(TStreamSaveContext& context) const;
        void Load(TStreamLoadContext& context);
    };

    //! Messages enqueued for the destination cell, ordered by id.
    DEFINE_BYREF_RW_PROPERTY(std::vector<TOutcomingMessage>, OutcomingMessages);

    //! The id of the next incoming message to be handled by Hydra.
    DEFINE_BYVAL_RW_PROPERTY(TMessageId, NextPersistentIncomingMessageId);

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);
};

void ToProto(
    NProto::TPersistentMailboxState* protoMailbox,
    const TPersistentMailboxState& mailbox);

void FromProto(
    TPersistentMailboxState* mailbox,
    const NProto::TPersistentMailboxState& protoMailbox);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
