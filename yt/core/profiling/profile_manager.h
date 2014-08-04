#pragma once

#include "public.h"

#include <core/actions/invoker.h>

#include <core/misc/public.h>

#include <core/ytree/yson_string.h>
#include <core/ytree/convert.h>

namespace NYT {
namespace NProfiling {

////////////////////////////////////////////////////////////////////////////////

//! Represents a sample that was enqueued to the profiler but did not
//! reach the storage yet.
struct TQueuedSample
{
    TQueuedSample()
        : Time(-1)
        , Value(-1)
    { }

    TCpuInstant Time;
    NYPath::TYPath Path;
    TValue Value;
    TTagIdList TagIds;

};

////////////////////////////////////////////////////////////////////////////////

//! A pre-registered key-value pair used to mark samples.
struct TTag
{
    Stroka Key;
    NYTree::TYsonString Value;
};

////////////////////////////////////////////////////////////////////////////////

//! A backend that controls all profiling activities.
/*!
 *  This class is a singleton, call #Get to obtain an instance.
 *
 *  Processing happens in a background thread that maintains
 *  a queue of all incoming (queued) samples and distributes them into buckets.
 *
 *  This thread also provides a invoker for executing various callbacks.
 */
class TProfileManager
{
public:
    //! Returns the singleton instance.
    static TProfileManager* Get();

    //! Starts profiling.
    /*!
     *  No samples are collected before this method is called.
     */
    void Start();

    //! Enqueues a new sample for processing.
    void Enqueue(const TQueuedSample& sample, bool selfProfiling);

    //! Returns the invoker associated with the profiler thread.
    IInvokerPtr GetInvoker() const;

    //! Returns the root of the tree with buckets.
    /*!
     *  The latter must only be accessed from the invoker returned by #GetInvoker.
     */
    NYTree::IMapNodePtr GetRoot() const;

    //! Returns a thread-safe service representing the tree with buckets.
    NYTree::IYPathServicePtr GetService() const;

    //! Registers a tag and returns its unique id.
    TTagId RegisterTag(const TTag& tag);

    //! Registers a tag and returns its unique id.
    template <class T>
    TTagId RegisterTag(const Stroka& key, const T& value)
    {
        TTag tag;
        tag.Key = key;
        tag.Value = NYTree::ConvertToYsonString(value);
        return RegisterTag(tag);
    }

    DECLARE_SINGLETON_MIXIN(TProfileManager, TStaticInstanceMixin);
    DECLARE_SINGLETON_PRIORITY(TProfileManager, 10);

private:
    TProfileManager();

    ~TProfileManager();

    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
