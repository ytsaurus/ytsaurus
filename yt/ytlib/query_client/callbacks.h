#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/misc/common.h>
#include <core/misc/error.h>

#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

//! Preparation context.
struct IPrepareCallbacks
{
    virtual ~IPrepareCallbacks()
    { }

    //! Returns an initial split for a given path.
    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(
        const NYPath::TYPath& path,
        TPlanContextPtr context) = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Evaluation context.
struct IEvaluateCallbacks
{
    virtual ~IEvaluateCallbacks()
    { }

    //! Returns a reader for a given split.
    virtual ISchemafulReaderPtr GetReader(
        const TDataSplit& split,
        TPlanContextPtr context) = 0;

};

////////////////////////////////////////////////////////////////////////////////

//! Coordination context.
struct ICoordinateCallbacks
    : public IEvaluateCallbacks
{
    virtual ~ICoordinateCallbacks()
    { }

    //! Checks if a given split could be partitioned further.
    virtual bool CanSplit(
        const TDataSplit& split) = 0;

    //! Reduces a given split to smaller partitions.
    virtual TFuture<TErrorOr<TDataSplits>> SplitFurther(
        const TDataSplit& split,
        TPlanContextPtr context) = 0;

    //! Regroups data splits so that each group could be effectively processed
    //! independently.
    /*  Typically we have three major cases:
     *  (1) Table composed of chunks; each chunk could be handled independently
     *      so no extra grouping is required.
     *  (2) Table composed of tablets; tablets coexist within a single tablet
     *      node, so it is more efficient to process them together.
     *  Resulting grouping is used to determine whether an extra coordination
     *  level is introduced. For non-sigleton groups first data split is used
     *  to collocate a coordinator.
     */
    virtual TGroupedDataSplits Regroup(
        const TDataSplits& splits,
        TPlanContextPtr context) = 0;

    //! Delegates fragment execution to be collocated with a given split.
    virtual std::pair<ISchemafulReaderPtr, TFuture<TErrorOr<TQueryStatistics>>> Delegate(
        const TPlanFragment& fragment,
        const TDataSplit& collocatedSplit) = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

