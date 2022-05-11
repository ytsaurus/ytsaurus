#pragma once

#include "public.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct TIOCounters
{
    i64 Bytes = 0;
    i64 IORequests = 0;

    void MergeFrom(TIOCounters other);
};

struct TIOEvent
{
    TIOCounters Counters;

    //! Tags that were propagated from other services are stored in baggage. It should usually come directly
    //! from TTraceContext.
    NYson::TYsonString Baggage;

    //! Local tags, i.e. all other tags that were not propagated in baggage.
    THashMap<TString, TString> LocalTags;
};

using TIOTagList = TCompactVector<std::pair<TString, TString>, 16>;

////////////////////////////////////////////////////////////////////////////////

//! All the methods here are thread-safe.
struct IIOTracker
    : public virtual TRefCounted
{
    //! Add the IO event to tracker. The event will be logged asynchronously, after some time passes.
    virtual void Enqueue(TIOEvent ioEvent) = 0;

    //! Add the IO event to tracker. Baggage will be filled automatically from the current TraceContext.
    void Enqueue(TIOCounters counters, THashMap<TString, TString> tags);

    //! Return true if the events can be accepted. Note that the return value of this method is not fully
    //! consistent with Enqueue. For example, IsEnabled() may return false, while Enqueue() will accept
    //! events. Eventually, if the config doesn't change, the return value of this method will correspond
    //! to reality.
    virtual bool IsEnabled() const = 0;

    virtual TIOTrackerConfigPtr GetConfig() const = 0;
    virtual void SetConfig(TIOTrackerConfigPtr config) = 0;

    //! These signals are used for testing purposes only. Do not subscribe to them in production code.
    //! The amount of handled events can be pretty large, so the code will slow down significantly.
    DECLARE_INTERFACE_SIGNAL(void(const TIOCounters&, const TIOTagList&), OnRawEventLogged);
    DECLARE_INTERFACE_SIGNAL(void(const TIOCounters&, const TIOTagList&), OnAggregateEventLogged);
    DECLARE_INTERFACE_SIGNAL(void(const TIOCounters&, const TIOTagList&), OnPathAggregateEventLogged);
};

DEFINE_REFCOUNTED_TYPE(IIOTracker)

////////////////////////////////////////////////////////////////////////////////

IIOTrackerPtr CreateIOTracker(TIOTrackerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
