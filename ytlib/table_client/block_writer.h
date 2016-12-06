#pragma once

#include "public.h"

#include <yt/ytlib/table_client/chunk_meta.pb.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NTableClient {

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

} // namespace NTableClient
} // namespace NYT
