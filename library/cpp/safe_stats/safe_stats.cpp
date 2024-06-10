#include "safe_stats.h"

using namespace NSFStats;

namespace {
    THolder<IThreadFactory::IThread> RunQueue(TStatsState::TFuncQueue* q) {
        return SystemThreadFactory()->Run([=]() {
            TThread::SetCurrentThreadName("safe_stats");
            while (true) {
                THolder<TStatsState::TFunc> func = q->Next();
                if (func == nullptr) {
                    return;
                }
                (*func)();
            }
        });
    }

    void FinishQueue(TStatsState::TFuncQueue* q, IThreadFactory::IThread* t) {
        THolder<TStatsState::TFunc> empty;
        q->ForceSchedule(std::move(empty));
        t->Join();
    }
}


TBasicMetric::TBasicMetric(const TString& name)
    : Name_(name)
{
}

const TString& TBasicMetric::Name() const {
    return Name_;
}



TStatsState::TContext::TContext(const TStats& s) noexcept
    : TContext(s.GetState())
{
}

void TStatsState::Apply(TContext& ctx) {
    if (ctx.F_->empty()) {
        return;
    }

    THolder<TFunc> f = MakeHolder<TFunc>([this, fs = std::move(ctx.F_)]() {
        if (fs == nullptr) {
            return;
        }
        with_lock (ML_) {
            for (const auto& f : *fs) {
                f(*this);
            }
        }
    });

    if (!F_) {
        (*f)();
    } else if (!F_->Schedule(f)) {
        ++QueueOverflowCounter_;
        (*f)();
    }
}

TStats::TStats()
    : State_(MakeAtomicShared<TStatsState>())
{

}

TStats::TStats(ui64 limit, bool reportQueueOverflows)
    : TStats()
{
    ReportQueueOverflows_ = reportQueueOverflows;
    if (!limit) {
        return;
    }

    State_->QueueHolder_.Reset(new TStatsState::TFuncQueue(limit));
    State_->F_ = State_->QueueHolder_.Get();

    T_ = RunQueue(State_->F_);
}

TStats::TStats(TStatsState::TFuncQueue* commonQueue)
    : TStats()
{
    State_->F_ = commonQueue;
}

TStats::~TStats() {
    if (State_ && State_->QueueHolder_) {
        FinishQueue(State_->F_, T_.Get());
    }
}

const TStatsStatePtr& TStats::GetState() const {
    return State_;
}

void TQueueStats::Add(TStringBuf name, double v) {
    with_lock (L_) {
        const auto& t = TInstant::Now();
        auto& d = Deques_[name];
        CleanUp(&d, t);
        d.push_back({t, v});
    }
}

TCommonStats::TCommonStats(ui64 limit) {
    Q_.Reset(new TStatsState::TFuncQueue(limit));
    T_ = RunQueue(Q_.Get());
}

TCommonStats::~TCommonStats() {
    Finish();
}

TStats TCommonStats::CreateStats() {
    return TStats{Q_.Get()};
}

void TCommonStats::Finish() {
    FinishQueue(Q_.Get(), T_.Get());
}
