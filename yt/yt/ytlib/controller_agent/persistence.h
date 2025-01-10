#pragma once

#include "serialize.h"

#include <yt/yt/client/table_client/serialize.h>

#include <yt/yt/core/phoenix/type_def.h>
#include <yt/yt/core/phoenix/type_decl.h>
#include <yt/yt/core/phoenix/public.h>
#include <yt/yt/core/phoenix/context.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NTableClient::TLoadContext
{
public:
    TLoadContext(
        IZeroCopyInput* input,
        NTableClient::TRowBufferPtr rowBuffer,
        ESnapshotVersion version);

    ESnapshotVersion GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NTableClient::TSaveContext
{
public:
    explicit TSaveContext(IZeroCopyOutput* output);

    ESnapshotVersion GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext, ESnapshotVersion>;
using IPersistent = NPhoenix2::ICustomPersistent<TSaveContext, TLoadContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
