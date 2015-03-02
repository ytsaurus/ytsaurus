#pragma once

#include "public.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <core/misc/ref.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TBlock
{
    std::vector<TSharedRef> Data;
    NProto::TBlockMeta Meta;
};

////////////////////////////////////////////////////////////////////////////////

struct IBlockWriter
{
    virtual i64 GetBlockSize() const = 0;
    virtual i64 GetRowCount() const = 0;

    virtual TBlock FlushBlock() = 0;

    virtual ~IBlockWriter()
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
