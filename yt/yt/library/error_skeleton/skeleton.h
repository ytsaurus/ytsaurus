#pragma once

#include <yt/yt/core/misc/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Perform recursive aggregation of error codes and messages over the error tree.
//! Result of this aggregation is suitable for error clustering in groups of
//! "similar" errors. Refer to yt/yt/library/error_skeleton/skeleton_ut.cpp for examples.
//!
//! This method builds skeleton from scratch by doing complete error tree traversal,
//! so calling it in computationally hot code is discouraged.
TString GetErrorSkeleton(const TError& error);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
