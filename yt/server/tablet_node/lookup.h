#pragma once

#include "public.h"

#include <ytlib/tablet_client/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Executes a bunch of row lookup requests. Request parameters are parsed via #reader,
//! response is written into #writer.
void LookupRows(
    IInvokerPtr poolInvoker,
    TTabletSnapshotPtr tabletSnapshot,
    TTimestamp timestamp,
    NTabletClient::TWireProtocolReader* reader,
    NTabletClient::TWireProtocolWriter* writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
