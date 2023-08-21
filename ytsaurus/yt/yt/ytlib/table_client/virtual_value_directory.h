#pragma once

#include "public.h"

#include <yt/yt/core/misc/shared_range.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Struct dedicated for storing constant values for virtual columns.
//! It owns a shared range of unversioned rows with their associated row buffer,
//! and also provides means of extracting them with proper reordering according
//! to external name table.
struct TVirtualValueDirectory
    : public TRefCounted
{
    TSharedRange<TUnversionedRow> Rows;
    TNameTablePtr NameTable;
    //! Schema for virtual values is needed for columnar chunk readers.
    TTableSchemaPtr Schema;
};

DEFINE_REFCOUNTED_TYPE(TVirtualValueDirectory)

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TVirtualValueDirectory* protoDirectory, const TVirtualValueDirectoryPtr& directory);
void FromProto(TVirtualValueDirectoryPtr* directory, const NProto::TVirtualValueDirectory& protoDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
