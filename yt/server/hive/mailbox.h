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
};

////////////////////////////////////////////////////////////////////////////////

class TMailbox
{
    // Persistent state.
    DEFINE_BYVAL_RO_PROPERTY(TCellGuid, CellGuid);

    DEFINE_BYVAL_RW_PROPERTY(int, FirstOutcomingMessageId);
    DEFINE_BYVAL_RW_PROPERTY(int, LastIncomingMessageId);
    DEFINE_BYVAL_RW_PROPERTY(int, InFlightMessageCount)

    DEFINE_BYREF_RW_PROPERTY(std::vector<Stroka>, OutcomingMessages);
    
    typedef std::map<int, Stroka> TIncomingMessageMap;
    DEFINE_BYREF_RW_PROPERTY(TIncomingMessageMap, IncomingMessages);

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
