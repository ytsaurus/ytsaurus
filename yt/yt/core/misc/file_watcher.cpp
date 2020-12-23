#include "file_watcher.h"

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/spinlock.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/assert.h>
#include <yt/core/misc/proc.h>

#ifdef _linux_
    #include <sys/inotify.h>
#endif

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

static const NLogging::TLogger Logger("FileWatcher");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNotificationHandle)

class TNotificationHandle
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, FD, -1);

public:
    TNotificationHandle()
    {
        FD_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        YT_VERIFY(FD_ >= 0);
    }

    ~TNotificationHandle()
    {
        YT_VERIFY(FD_ >= 0);
        ::close(FD_);
    }

    int Poll()
    {
        YT_VERIFY(FD_ >= 0);

        char buffer[sizeof(struct inotify_event) + NAME_MAX + 1];
        auto rv = HandleEintr(::read, FD_, buffer, sizeof(buffer));
        if (rv < 0) {
            if (errno != EAGAIN) {
                YT_LOG_DEBUG(
                    TError::FromSystem(errno),
                    "Unable to poll inotify descriptor (FD: %v)",
                    FD_);
            }
        } else if (rv > 0) {
            YT_ASSERT(rv >= sizeof(struct inotify_event));
            struct inotify_event* event = reinterpret_cast<struct inotify_event*>(buffer);

            if (event->mask & IN_ATTRIB) {
                YT_LOG_DEBUG(
                    "Watch has triggered IN_ATTRIB (FD: %v, WD: %v)",
                    FD_,
                    event->wd);
            }
            if (event->mask & IN_DELETE_SELF) {
                YT_LOG_DEBUG(
                    "Watch has triggered IN_DELETE_SELF (FD: %v, WD: %v)",
                    FD_,
                    event->wd);
            }
            if (event->mask & IN_MOVE_SELF) {
                YT_LOG_DEBUG(
                    "Watch has triggered IN_MOVE_SELF (FD: %v, WD: %v)",
                    FD_,
                    event->wd);
            }
            if (event->mask & IN_MODIFY) {
                YT_LOG_DEBUG(
                    "Watch has triggered IN_MODIFY (FD: %v, WD: %v)",
                    FD_,
                    event->wd);
            }
            if (event->mask & ~IN_IGNORED) {
                return event->wd;
            }
        } else {
            // Do nothing.
        }
        return -1;
    }
};

DEFINE_REFCOUNTED_TYPE(TNotificationHandle)

////////////////////////////////////////////////////////////////////////////////

class TFileWatch
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(int, WD, -1);

public:
    TFileWatch(
        TNotificationHandlePtr handle,
        TString path,
        EFileWatchMode mode,
        TClosure callback)
        : Handle_(std::move(handle))
        , Path_(std::move(path))
        , Mode_(mode)
        , Callback_(std::move(callback))
    {
        DoCreate();
    }

    ~TFileWatch()
    {
        DoDrop();
    }

    void Drop()
    {
        DoDrop();
    }

    void Run()
    {
        YT_LOG_DEBUG("Running file watch handler (WD: %v, Path: %v)",
            WD_,
            Path_);
        Callback_();
        // Reinitialize watch to hook to a possibly recreated file.
        DoDrop();
        DoCreate();
    }

private:
    const TNotificationHandlePtr Handle_;
    const TString Path_;
    const EFileWatchMode Mode_;
    const TClosure Callback_;

    YT_DECLARE_SPINLOCK(TSpinLock, SpinLock_);

    void DoCreate()
    {
        auto guard = Guard(SpinLock_);
        YT_VERIFY(WD_ < 0);
        WD_ = inotify_add_watch(
            Handle_->GetFD(),
            Path_.c_str(),
            static_cast<ui32>(Mode_));

        if (WD_ < 0) {
            YT_LOG_ERROR(TError::FromSystem(errno), "Error registering file watch (Path: %v)",
                Path_);
            WD_ = -1;
        } else if (WD_ > 0) {
            YT_LOG_DEBUG("Registered file watch (WD: %v, Path: %v)",
                WD_,
                Path_);
        } else {
            YT_ABORT();
        }
    }

    void DoDrop()
    {
        auto guard = Guard(SpinLock_);
        if (WD_ > 0) {
            YT_LOG_DEBUG("Unregistered file watch (WD: %v, Path: %v)",
                WD_,
                Path_);
            inotify_rm_watch(Handle_->GetFD(), WD_);
        }
        WD_ = -1;
    }
};

DEFINE_REFCOUNTED_TYPE(TFileWatch)

////////////////////////////////////////////////////////////////////////////////

class TFileWatcher
    : public IFileWatcher
{
public:
    TFileWatcher(
        IInvokerPtr invoker,
        TDuration checkPeriod)
        : NotificationHandle_(New<TNotificationHandle>())
        , CheckExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TFileWatcher::OnCheck, MakeWeak(this)),
            checkPeriod))
    { }

    virtual void Start() override
    {
        CheckExecutor_->Start();
    }

    virtual void Stop() override
    {
        CheckExecutor_->Stop();
    }

    virtual TFileWatchPtr CreateWatch(
        TString path,
        EFileWatchMode mode,
        TClosure callback) override
    {
        auto guard = Guard(SpinLock_);

        auto watch = New<TFileWatch>(
            NotificationHandle_,
            std::move(path),
            mode,
            std::move(callback));

        if (watch->GetWD() >= 0) {
            YT_VERIFY(WDToWatch_.emplace(watch->GetWD(), watch).second);
        }

        return watch;
    }

    virtual void DropWatch(const TFileWatchPtr& watch) override
    {
        if (auto wd = watch->GetWD(); wd >= 0) {
            auto guard = Guard(SpinLock_);
            WDToWatch_.erase(wd);
        }

        watch->Drop();
    }

private:
    const TNotificationHandlePtr NotificationHandle_;
    const TPeriodicExecutorPtr CheckExecutor_;

    YT_DECLARE_SPINLOCK(TSpinLock, SpinLock_);
    THashMap<int, TFileWatchPtr> WDToWatch_;

    void OnCheck()
    {
        THashSet<TFileWatchPtr> triggeredWatches;
        {
            auto guard = Guard(SpinLock_);

            if (!NotificationHandle_) {
                return;
            }

            while (true) {
                auto wd = NotificationHandle_->Poll();
                if (wd < 0) {
                    break;
                }

                auto it = WDToWatch_.find(wd);
                if (it == WDToWatch_.end()) {
                    continue;
                }

                triggeredWatches.insert(it->second);
                WDToWatch_.erase(it);
            }
        }

        if (triggeredWatches.empty()) {
            return;
        }

        for (const auto& watch : triggeredWatches) {
            watch->Run();
        }

        {
            auto guard = Guard(SpinLock_);
            for (const auto& watch : triggeredWatches) {
                if (auto wd = watch->GetWD(); wd >= 0) {
                    WDToWatch_.emplace(wd, watch);
                }
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TFileWatcher)

IFileWatcherPtr CreateFileWatcher(
    IInvokerPtr invoker,
    TDuration checkPeriod)
{
    return New<TFileWatcher>(
        std::move(invoker),
        checkPeriod);
}

#else

class TFileWatch
    : public TRefCounted
{ };

DEFINE_REFCOUNTED_TYPE(TFileWatch)

class TFileWatcher
    : public IFileWatcher
{
public:
    virtual void Start() override
    { }

    virtual void Stop() override
    { }

    virtual TFileWatchPtr CreateWatch(
        TString /*path*/,
        EFileWatchMode /*mode*/,
        TClosure /*callback*/) override
    {
        return New<TFileWatch>();
    }

    virtual void DropWatch(const TFileWatchPtr& /*watch*/) override
    { }
};

DEFINE_REFCOUNTED_TYPE(TFileWatcher)

IFileWatcherPtr CreateFileWatcher(
    IInvokerPtr /*invoker*/,
    TDuration /*checkPeriod*/)
{
    return New<TFileWatcher>();
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
