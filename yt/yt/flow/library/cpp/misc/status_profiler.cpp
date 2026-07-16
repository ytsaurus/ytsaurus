#include "status_profiler.h"

#include <yt/yt/core/actions/invoker.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TStatusProfilerUnregistrator = std::function<void()>;

DEFINE_REFCOUNTED_TYPE(IStatusErrorState);
DEFINE_REFCOUNTED_TYPE(IStatusProfiler);

struct TStatusLoggingContext
    : public TRefCounted
{
    NLogging::TLogger Logger;
    TStatusProfilerLoggingOptions Options;
    NProfiling::TProfiler Profiler;
};

using TStatusLoggingContextPtr = TIntrusivePtr<TStatusLoggingContext>;

// Older flips are dropped, undercounting a component that flaps for the whole window.
constexpr int MaxTrackedTransitions = 256;

////////////////////////////////////////////////////////////////////////////////

class TStatusErrorStateImpl
    : public IStatusErrorState
{
public:
    // #unregistrator holds parent alive, so the root can reach this node. Also it used to unlink from parent when object is destroyed.
    TStatusErrorStateImpl(
        TStatusProfilerUnregistrator unregistrator,
        std::string prefix,
        TStatusLoggingContextPtr loggingContext)
        : Unregistrator_(std::move(unregistrator))
        , Prefix_(std::move(prefix))
        , LoggingContext_(std::move(loggingContext))
        , BrokenGauge_(LoggingContext_->Profiler.WithTag("path", Prefix_).Gauge("/status_profiler/broken"))
    {
        BrokenGauge_.Update(0.0);
    }

    ~TStatusErrorStateImpl() override
    {
        if (Unregistrator_) {
            Unregistrator_();
        }
    }

    void SetError(TError error) override
    {
        const TInstant now = TInstant::Now();

        TPendingLog pending;
        {
            auto guard = Guard(Lock_);

            const bool wasBroken = Status_.IsOK.has_value() && !Status_.IsOK.value();
            const bool nowBroken = !error.IsOK();

            Status_.IsOK = error.IsOK();
            if (!nowBroken) {
                Status_.LastOKTime = now;
            } else if (!wasBroken) {
                LastBreakTime_ = now;
            }
            if (Error_.IsOK() != error.IsOK()) {
                Status_.LastStateChangeTime = now;
            }
            std::swap(Error_, error);

            BrokenGauge_.Update(nowBroken ? 1.0 : 0.0);
            pending = UpdateLoggingState(now, wasBroken, nowBroken);
        }

        EmitLog(pending);
    }

    void ClearError() override
    {
        SetError(TError());
    }

    TStatus GetStatus() const override
    {
        auto guard = Guard(Lock_);
        return Status_;
    }

    void CollectErrors(THashMap<std::string, TError>& result) const
    {
        auto guard = Guard(Lock_);
        if (!Error_.IsOK()) {
            result[Prefix_] = Error_;
        }
    }

    void ReportStatus(TInstant now)
    {
        TPendingReport report;
        {
            auto guard = Guard(Lock_);
            report = BuildReport(now);
        }
        EmitReport(report);
    }

private:
    const TStatusProfilerUnregistrator Unregistrator_;
    const std::string Prefix_;
    const TStatusLoggingContextPtr LoggingContext_;
    NProfiling::TGauge BrokenGauge_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TError Error_;
    TStatus Status_;

    struct TTransition
    {
        TInstant Time;
        bool ToError;
    };

    std::deque<TTransition> Transitions_;
    TInstant LastBreakTime_;
    TInstant LastErrorLogTime_;
    TInstant LastRecoverLogTime_;

    struct TPendingLog
    {
        enum class EKind
        {
            None,
            Break,
            StillBroken,
            Recover,
        };
        EKind Kind = EKind::None;
        TError Error;
        TDuration BrokenFor;
    };

    struct TPendingReport
    {
        bool ShouldReport = false;
        bool Broken = false;
        TError Error;
        std::optional<TDuration> TimeSinceLastOK;
        TDuration ErrorTimeInWindow;
        int BreakCountInWindow = 0;
        TDuration Window;
    };

    TPendingLog UpdateLoggingState(TInstant now, bool wasBroken, bool nowBroken)
    {
        const auto throttle = LoggingContext_->Options.LogThrottle;

        const bool flipped = wasBroken != nowBroken;
        if (flipped) {
            RecordTransition(now, nowBroken);
        }

        TPendingLog pending;
        if (nowBroken) {
            if (now >= LastErrorLogTime_ + throttle) {
                LastErrorLogTime_ = now;
                pending.Kind = flipped ? TPendingLog::EKind::Break : TPendingLog::EKind::StillBroken;
                pending.Error = Error_;
                pending.BrokenFor = now - LastBreakTime_;
            }
        } else if (flipped) {
            // A separate throttle so a break/recover pair always logs both.
            if (now >= LastRecoverLogTime_ + throttle) {
                LastRecoverLogTime_ = now;
                pending.Kind = TPendingLog::EKind::Recover;
                pending.BrokenFor = now - LastBreakTime_;
            }
        }
        return pending;
    }

    void RecordTransition(TInstant now, bool toError)
    {
        Transitions_.push_back({.Time = now, .ToError = toError});
        while (std::ssize(Transitions_) > MaxTrackedTransitions) {
            Transitions_.pop_front();
        }
    }

    TPendingReport BuildReport(TInstant now)
    {
        const auto window = LoggingContext_->Options.StatusWindow;
        const auto windowStart = now - window;

        while (!Transitions_.empty() && Transitions_.front().Time < windowStart) {
            Transitions_.pop_front();
        }

        const bool broken = Status_.IsOK.has_value() && !Status_.IsOK.value();
        if (!broken && Transitions_.empty()) {
            return {};
        }

        // The earliest in-window flip's polarity gives the state entering the window.
        bool entryBroken = Transitions_.empty() ? broken : !Transitions_.front().ToError;
        TDuration errorTime;
        int breakCount = 0;
        TInstant cursor = windowStart;
        bool currentlyBroken = entryBroken;
        for (const auto& transition : Transitions_) {
            if (currentlyBroken) {
                errorTime += transition.Time - cursor;
            }
            cursor = transition.Time;
            currentlyBroken = transition.ToError;
            if (transition.ToError) {
                ++breakCount;
            }
        }
        if (currentlyBroken) {
            errorTime += now - cursor;
        }

        TPendingReport report;
        report.ShouldReport = true;
        report.Broken = broken;
        report.ErrorTimeInWindow = errorTime;
        report.BreakCountInWindow = breakCount;
        report.Window = window;
        if (broken) {
            report.Error = Error_;
            if (Status_.LastOKTime != TInstant::Zero()) {
                report.TimeSinceLastOK = now - Status_.LastOKTime;
            }
        }
        return report;
    }

    void EmitLog(const TPendingLog& pending)
    {
        if (pending.Kind == TPendingLog::EKind::None) {
            return;
        }
        const auto& Logger = LoggingContext_->Logger;
        switch (pending.Kind) {
            case TPendingLog::EKind::Break:
                YT_TLOG_WARNING("Component became broken")
                    .With("Component", Prefix_)
                    .With(pending.Error);
                break;
            case TPendingLog::EKind::StillBroken:
                YT_TLOG_WARNING("Component is still broken")
                    .With("Component", Prefix_)
                    .With("BrokenFor", pending.BrokenFor)
                    .With(pending.Error);
                break;
            case TPendingLog::EKind::Recover:
                YT_TLOG_INFO("Component recovered")
                    .With("Component", Prefix_)
                    .With("WasBrokenFor", pending.BrokenFor);
                break;
            case TPendingLog::EKind::None:
                break;
        }
    }

    void EmitReport(const TPendingReport& report)
    {
        if (!report.ShouldReport) {
            return;
        }
        const auto& Logger = LoggingContext_->Logger;
        if (report.Broken) {
            YT_TLOG_WARNING("Component status report")
                .With("Component", Prefix_)
                .With("Status", "Broken")
                .With("TimeSinceLastOK", report.TimeSinceLastOK)
                .With("ErrorTimeInWindow", report.ErrorTimeInWindow)
                .With("BreakCountInWindow", report.BreakCountInWindow)
                .With("Window", report.Window)
                .With(report.Error);
        } else {
            YT_TLOG_INFO("Component status report")
                .With("Component", Prefix_)
                .With("Status", "Recovered")
                .With("ErrorTimeInWindow", report.ErrorTimeInWindow)
                .With("BreakCountInWindow", report.BreakCountInWindow)
                .With("Window", report.Window);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStatusProfilerImpl
    : public NYT::NFlow::IStatusProfiler
{
    using TRegistrationCookie = ui64;

    struct TChildHooks
    {
        std::function<void(THashMap<std::string, TError>&)> CollectErrors;
        std::function<void(TInstant)> ReportStatus;
    };

public:
    // #unregistrator holds parent alive, so the root can reach this node. Also it used to unlink from parent when object is destroyed.
    TStatusProfilerImpl(
        TStatusProfilerUnregistrator unregistrator,
        std::string prefix,
        TStatusLoggingContextPtr loggingContext)
        : Unregistrator_(std::move(unregistrator))
        , Prefix_(std::move(prefix))
        , LoggingContext_(std::move(loggingContext))
    { }

    ~TStatusProfilerImpl() override
    {
        if (ReportExecutor_) {
            YT_UNUSED_FUTURE(ReportExecutor_->Stop());
        }
        if (Unregistrator_) {
            Unregistrator_();
        }
    }

    void SetReportExecutor(NConcurrency::TPeriodicExecutorPtr executor)
    {
        ReportExecutor_ = std::move(executor);
    }

    template <typename TNode>
    TIntrusivePtr<TNode> CreateChild(TStringBuf nodePrefix)
    {
        auto guard = Guard(Lock_);
        auto cookie = RegistrationCookieCounter_++;
        auto node = New<TNode>(
            [this, this_ = MakeStrong(this), cookie] {
                auto guard = Guard(Lock_);
                Children_.erase(cookie);
            },
            Prefix_ + std::string(nodePrefix),
            LoggingContext_);
        Children_[cookie] = TChildHooks{
            .CollectErrors = [weakNode = MakeWeak(node)] (THashMap<std::string, TError>& result) {
                if (auto node = weakNode.Lock()) {
                    node->CollectErrors(result);
                }
            },
            .ReportStatus = [weakNode = MakeWeak(node)] (TInstant now) {
                if (auto node = weakNode.Lock()) {
                    node->ReportStatus(now);
                }
            },
        };
        return node;
    }

    NYT::NFlow::IStatusErrorStatePtr ErrorState(TStringBuf name) override
    {
        return CreateChild<TStatusErrorStateImpl>(name);
    }

    NYT::NFlow::IStatusProfilerPtr WithPrefix(TStringBuf prefix) override
    {
        return CreateChild<TStatusProfilerImpl>(prefix);
    }

    NYT::NFlow::IStatusProfiler::TUnitedProfilerStatus GetStatus() const override
    {
        NYT::NFlow::IStatusProfiler::TUnitedProfilerStatus status;
        CollectErrors(status.Errors);
        return status;
    }

    void CollectErrors(THashMap<std::string, TError>& result) const
    {
        auto collectors = SnapshotHooks(&TChildHooks::CollectErrors);
        for (const auto& collect : collectors) {
            collect(result);
        }
    }

    void ReportStatus(TInstant now)
    {
        auto reporters = SnapshotHooks(&TChildHooks::ReportStatus);
        for (const auto& report : reporters) {
            report(now);
        }
    }

private:
    const TStatusProfilerUnregistrator Unregistrator_;
    const std::string Prefix_;
    const TStatusLoggingContextPtr LoggingContext_;
    NConcurrency::TPeriodicExecutorPtr ReportExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TRegistrationCookie RegistrationCookieCounter_ = 0;
    THashMap<TRegistrationCookie, TChildHooks> Children_;

    template <typename THook>
    std::vector<THook> SnapshotHooks(THook TChildHooks::*member) const
    {
        std::vector<THook> hooks;
        auto guard = Guard(Lock_);
        hooks.reserve(Children_.size());
        for (const auto& [cookie, childHooks] : Children_) {
            hooks.push_back(childHooks.*member);
        }
        return hooks;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IStatusErrorState);
DEFINE_REFCOUNTED_TYPE(IStatusProfiler);

////////////////////////////////////////////////////////////////////////////////

IStatusProfilerPtr CreateStatusProfiler(
    const IInvokerPtr& invoker,
    const NLogging::TLogger& logger,
    const TStatusProfilerLoggingOptions& options,
    const NProfiling::TProfiler& profiler)
{
    auto loggingContext = New<TStatusLoggingContext>();
    loggingContext->Logger = logger;
    loggingContext->Options = options;
    loggingContext->Profiler = profiler;

    auto statusProfiler = New<TStatusProfilerImpl>(TStatusProfilerUnregistrator(), std::string(), loggingContext);
    if (options.ReportPeriod != TDuration::Zero()) {
        auto executor = New<NConcurrency::TPeriodicExecutor>(
            invoker,
            BIND([weakProfiler = MakeWeak(statusProfiler)] {
                if (auto profiler = weakProfiler.Lock()) {
                    profiler->ReportStatus(TInstant::Now());
                }
            }),
            options.ReportPeriod);
        // Weak ref: the tree's destructor stops the executor.
        statusProfiler->SetReportExecutor(executor);
        executor->Start();
    }
    return statusProfiler;
}

IStatusProfilerPtr CreateSyncStatusProfiler(const NLogging::TLogger& logger)
{
    return CreateStatusProfiler(
        GetSyncInvoker(),
        logger,
        TStatusProfilerLoggingOptions{
            .LogThrottle = TDuration::Zero(),
            .ReportPeriod = TDuration::Zero(),
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
