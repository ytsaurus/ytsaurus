#pragma once

#include "public.h"

#include <core/misc/common.h>
#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/schema.h>

#include <ytlib/object_client/public.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////
// In use.

typedef NChunkClient::NProto::TChunkSpec TDataSplit;

Stroka GetObjectIdFromDataSplit(const TDataSplit& split);

static const int TypicalTableCount = 2;

using NVersionedTableClient::EColumnType;
using NVersionedTableClient::NProto::TColumnSchema;
typedef NVersionedTableClient::NProto::TTableSchemaExt TTableSchema;

////////////////////////////////////////////////////////////////////////////////
// Hooks for other means.

typedef void* IMegaReaderPtr;
typedef void* IMegaWriterPtr;

class IExecutor
    : public TRefCounted
{
public:
    virtual IMegaReaderPtr Execute(const TQueryFragment& fragment) = 0;
};

struct IPrepareCallbacks
{
    virtual ~IPrepareCallbacks()
    { }

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const NYPath::TYPath& path) = 0;
};

struct ICoordinateCallbacks
{
    virtual ~ICoordinateCallbacks()
    { }

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(const TDataSplit& split) = 0;
    virtual IExecutorPtr GetColocatedExecutor(const TDataSplit& split) = 0;
    virtual IExecutorPtr GetLocalExecutor() = 0;

};

struct IEvaluateCallbacks
{
    virtual ~IEvaluateCallbacks()
    { }

    virtual IMegaReaderPtr GetReader(const TDataSplit& split) = 0;
    virtual IMegaWriterPtr GetWriter() = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

