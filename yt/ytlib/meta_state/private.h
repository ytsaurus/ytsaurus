#pragma once

#include "public.h"

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger MetaStateLogger;
extern NProfiling::TProfiler MetaStateProfiler;

////////////////////////////////////////////////////////////////////////////////

//! A special value indicating that the number of records in the previous
//! changelog is undetermined since there is no previous changelog.
/*!
 *  \see TRecovery
 */
const i32 NonexistingPrevRecordCount = -1;

//! A special value indicating that the number of records in the previous changelog
//! is unknown.
/*!
 *  \see TRecovery
 */
const i32 UnknownPrevRecordCount = -2;

//! A special value indicating that no snapshot id is known.
//! is unknown.
/*!
 *  \see TSnapshotStore
 */
const i32 NonexistingSnapshotId = -1;

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
