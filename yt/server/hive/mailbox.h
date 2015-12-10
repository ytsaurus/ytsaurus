#pragma once

#include "public.h"

#include <yt/server/hive/hive_manager.pb.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

#include <yt/core/rpc/public.h>

#include <yt/core/tracing/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TMailbox
    : public NHydra::TEntityBase
    , public TRefTracked<TMailbox>
{
public:
    // Persistent state.
    DEFINE_BYVAL_RO_PROPERTY(TCellId, CellId);

    DEFINE_BYVAL_RW_PROPERTY(TMessageId, FirstOutcomingMessageId);
    DEFINE_BYVAL_RW_PROPERTY(TMessageId, LastIncomingMessageId);
    DEFINE_BYVAL_RW_PROPERTY(bool, PostMessagesInFlight)

    DEFINE_BYREF_RW_PROPERTY(std::vector<NProto::TEncapsulatedMessage>, OutcomingMessages);
    
    typedef std::map<TMessageId, NProto::TEncapsulatedMessage> TIncomingMessageMap;
    DEFINE_BYREF_RW_PROPERTY(TIncomingMessageMap, IncomingMessages);

    // Transient state.
    DEFINE_BYVAL_RW_PROPERTY(bool, Connected);

    struct TSyncRequest
    {
        TMessageId MessageId;
        TPromise<void> Promise;
    };

    typedef std::map<TMessageId, TSyncRequest> TSyncRequestMap;
    DEFINE_BYREF_RW_PROPERTY(TSyncRequestMap, SyncRequests);

public:
    explicit TMailbox(const TCellId& cellId);

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
