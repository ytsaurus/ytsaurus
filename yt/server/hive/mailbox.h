#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <core/rpc/public.h>

#include <ytlib/hydra/hydra_manager.pb.h>

#include <server/hydra/public.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

struct TMessage
{
    Stroka Type;
    TSharedRef Data;

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

class TMailbox
{
    // Persistent state.
    DEFINE_BYVAL_RO_PROPERTY(TCellGuid, CellGuid);
    DEFINE_BYVAL_RW_PROPERTY(int, FirstPendingMessageId);
    DEFINE_BYVAL_RW_PROPERTY(int, LastReceivedMessageId);
    DEFINE_BYVAL_RW_PROPERTY(int, InFlightMessageCount)
    DEFINE_BYREF_RW_PROPERTY(std::vector<TMessage>, PendingMessages);

    // Transient state.
    DEFINE_BYVAL_RW_PROPERTY(bool, Connected);

public:
    explicit TMailbox(const TCellGuid& cellGuid);

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
