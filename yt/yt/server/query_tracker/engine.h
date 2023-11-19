#pragma once

#include "private.h"

#include <yt/yt/ytlib/query_tracker_client/public.h>
#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/misc/error.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

struct IQueryHandler
    : public TRefCounted
{
    //! A call that must start query in the underlying system.
    virtual void Start() = 0;

    //! A call that must abort query in the underlying system.
    virtual void Abort() = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueryHandler)

////////////////////////////////////////////////////////////////////////////////

struct IQueryEngine
    : public TRefCounted
{
    virtual IQueryHandlerPtr StartQuery(NQueryTrackerClient::NRecords::TActiveQuery activeQuery) = 0;

    virtual void OnDynamicConfigChanged(const TEngineConfigBasePtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQueryEngine)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
