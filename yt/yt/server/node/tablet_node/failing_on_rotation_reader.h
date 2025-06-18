#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/tablet.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Creates a reader that stores actual reader and protects from stale reads in ordered tables
//! by comparing OrderedDynamicStoreRotateEpoch before/after calling Read() on actual reader
ISchemafulUnversionedReaderPtr CreateFailingOnRotationReader(
    ISchemafulUnversionedReaderPtr reader,
    NTabletNode::TTabletSnapshotPtr tabletSnapshot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
