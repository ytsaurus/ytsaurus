#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/misc/common.h>
#include <core/misc/error.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IPrepareCallbacks
{
    virtual ~IPrepareCallbacks()
    { }

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(
        const NYPath::TYPath& path,
        TPlanContextPtr context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IEvaluateCallbacks
{
    virtual ~IEvaluateCallbacks()
    { }

    virtual ISchemedReaderPtr GetReader(
        const TDataSplit& dataSplit,
        TPlanContextPtr context) = 0;

};

////////////////////////////////////////////////////////////////////////////////

struct ICoordinateCallbacks
    : public IEvaluateCallbacks
{
    virtual ~ICoordinateCallbacks()
    { }

    virtual bool CanSplit(
        const TDataSplit& dataSplit) = 0;

    virtual TFuture<TErrorOr<TDataSplits>> SplitFurther(
        const TDataSplit& dataSplit,
        TPlanContextPtr context) = 0;

    virtual TLocationToDataSplits GroupByLocation(
        const TDataSplits& dataSplits,
        TPlanContextPtr context) = 0;

    virtual ISchemedReaderPtr Delegate(
        const TPlanFragment& fragment,
        const Stroka& location) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

