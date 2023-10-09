#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Creates an adapter acting on top of another changelog whose construction may
//! take some time.
/*!
 *  The adapter serves most of IChangelog method with minimum latency
 *  (i.e. only blocks if the method is supposed to be blocking).
 *
 *  The underlying changelog must be constructed empty, non-sealed.
 *  This is assumed while answering the relevant requests.
 */
IChangelogPtr CreateLazyChangelog(
    int changelogId,
    TFuture<IChangelogPtr> futureChangelog);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
