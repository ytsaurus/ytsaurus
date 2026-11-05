#pragma once

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Drops the strong reference held by #ptr (retaining only a weak one) and schedules
//! a deferred check that fires after #timeout. If by that time the object has not
//! expired -- i.e. someone still holds a (possibly circular) reference to it -- an
//! alert is logged via #logger.
/*!
 *  This is a finalization-time sanity check: move the last strong reference to an
 *  object here to ensure that finalizing it actually releases all references and
 *  that no reference cycle keeps it alive. The reference is consumed (and dropped)
 *  by this call, so the only thing that may keep the object alive afterwards is an
 *  unexpected reference.
 *
 *  The check merely locks a weak reference and thus never keeps the object alive on
 *  its own. It runs in the delayed executor thread.
 */
template <class T>
void VerifyEventualExpiration(
    TIntrusivePtr<T>&& ptr,
    NLogging::TLogger logger,
    TDuration timeout = TDuration::Minutes(1));

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define EXPIRATION_VERIFIER_INL_H_
#include "expiration_verifier-inl.h"
#undef EXPIRATION_VERIFIER_INL_H_
