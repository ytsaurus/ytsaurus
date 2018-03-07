#pragma once

#include "public.h"

#include <yt/core/misc/property.h>
#include <yt/core/misc/phoenix.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NPhoenix::TSaveContext
{ };

class TLoadContext
    : public NPhoenix::TLoadContext
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TRowBufferPtr, RowBuffer);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
