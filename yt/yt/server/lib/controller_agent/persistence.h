#pragma once

#include "serialize.h"

#include <yt/yt/client/table_client/serialize.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NTableClient::TLoadContext
{
    using NTableClient::TLoadContext::TLoadContext;

public:
    ESnapshotVersion GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NTableClient::TSaveContext
{
    using NTableClient::TSaveContext::TSaveContext;

public:
    ESnapshotVersion GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext, ESnapshotVersion>;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
