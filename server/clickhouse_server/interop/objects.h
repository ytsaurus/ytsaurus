#pragma once

#include <util/datetime/base.h>
#include <util/system/types.h>

namespace NInterop {

// Objects in storage

////////////////////////////////////////////////////////////////////////////////

enum class EObjectType
{
    Table = 0,
    File,
    Document,
    Other
};

////////////////////////////////////////////////////////////////////////////////

using TRevision = i64;

////////////////////////////////////////////////////////////////////////////////

// TODO: customize set of attributes

struct TObjectAttributes
{
    EObjectType Type;
    TRevision Revision;
    TInstant LastModificationTime;
};

} // namespace NInterop
