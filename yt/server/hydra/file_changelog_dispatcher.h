#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

#include <core/actions/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

//! Provides a factory for creating new and opening existing file changelogs.
//! Manages a background thread that keeps track of unflushed changelogs and
//! issues flush requests periodically.
class TFileChangelogDispatcher
    : public TRefCounted
    , public IShutdownable
{
public:
    explicit TFileChangelogDispatcher(const Stroka& threadName);

    ~TFileChangelogDispatcher();

    virtual void Shutdown() override;

    //! Returns the invoker managed by the dispatcher.
    IInvokerPtr GetInvoker();

    //! Synchronously creates a new changelog.
    IChangelogPtr CreateChangelog(
        const Stroka& path,
        const NProto::TChangelogMeta& meta,
        TFileChangelogConfigPtr config);

    //! Synchronously opens an existing changelog.
    IChangelogPtr OpenChangelog(
        const Stroka& path,
        TFileChangelogConfigPtr config);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    friend class TFileChangelog;
    friend class TFileChangelogQueue;

    TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TFileChangelogDispatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
