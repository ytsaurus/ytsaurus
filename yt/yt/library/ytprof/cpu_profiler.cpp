#include "cpu_profiler.h"
#include "symbolize.h"

#include <link.h>

#include <util/system/yield.h>

#include <contrib/libs/libunwind/include/libunwind.h>

#include <csignal>

#include <util/generic/yexception.h>

namespace NYT::NProf {

////////////////////////////////////////////////////////////////////////////////

TCpuSample::operator size_t() const
{
    size_t hash = 0;
    for (auto [startIP, ip] : Backtrace) {
        hash ^= reinterpret_cast<size_t>(startIP);
        hash = (hash << 1) | (hash >> 63);

        hash ^= reinterpret_cast<size_t>(ip);
        hash = (hash << 1) | (hash >> 63);
    }

    return hash;
}

////////////////////////////////////////////////////////////////////////////////



////////////////////////////////////////////////////////////////////////////////

TCpuProfiler::TCpuProfiler(TCpuProfilerOptions options)
    : Options_(options)
    , Queue_(options.RingBufferLogSize)
{
    VDSORange_ = GetVDSORange();
}

TCpuProfiler::~TCpuProfiler()
{
    Stop();
}

void TCpuProfiler::Start()
{
    StartTimer();

    BackgroundThread_ = std::thread([this] {
        DequeueSamples();
    });
}

std::atomic<TCpuProfiler*> TCpuProfiler::ActiveProfiler_;
std::atomic<bool> TCpuProfiler::HandlingSigprof_;

void TCpuProfiler::SigProfHandler(int /* sig */, siginfo_t* info, void* ucontext)
{
    while (HandlingSigprof_.exchange(true)) {
        SchedYield();
    }

    auto profiler = ActiveProfiler_.load();
    if (profiler) {
        profiler->OnSigProf(info, reinterpret_cast<ucontext_t*>(ucontext));
    }

    HandlingSigprof_.store(false);
}

void TCpuProfiler::StartTimer()
{
    ActiveProfiler_ = this;

    struct sigaction sig;
    sig.sa_flags = SA_SIGINFO | SA_RESTART;
    sigemptyset(&sig.sa_mask);
    sig.sa_sigaction = &TCpuProfiler::SigProfHandler;

    if (sigaction(SIGPROF, &sig, NULL) != 0) {
        throw TSystemError(LastSystemError());
    }

    itimerval period;
    period.it_value.tv_sec = 0;
    period.it_value.tv_usec = 1000000 / Options_.SamplingFrequency;
    period.it_interval = period.it_value;

    if (setitimer(ITIMER_PROF, &period, nullptr) != 0) {
        throw TSystemError(LastSystemError());
    }
}

void TCpuProfiler::StopTimer()
{
    itimerval period{};
    if (setitimer(ITIMER_PROF, &period, nullptr) != 0) {
        throw TSystemError(LastSystemError());
    }

    struct sigaction sig;
    sig.sa_flags = 0;
    sigemptyset(&sig.sa_mask);
    sig.sa_handler = SIG_DFL;

    if (sigaction(SIGPROF, &sig, NULL) != 0) {
        throw TSystemError(LastSystemError());
    }

    ActiveProfiler_ = nullptr;
    while (HandlingSigprof_ > 0) {
        SchedYield();
    }
}

void TCpuProfiler::Stop()
{
    if (Stop_) {
        return;
    }

    Stop_ = true;
    StopTimer();
    BackgroundThread_.join();
}

class TUWCursor
{
public:
    TUWCursor()
    {
        if (unw_getcontext(&Context_) != 0) {
            End_ = true;
            return;
        }

        if (unw_init_local(&Cursor_, &Context_) != 0) {
            End_ = true;
            return;
        }

        ReadIP();
    }

    bool IsEnd()
    {
        return End_;
    }

    bool Next()
    {
        if (End_) {
            return false;
        }

        StepReturnValue_ = unw_step(&Cursor_);
        if (StepReturnValue_ <= 0) {
            End_ = true;
            return false;
        }

        ReadIP();
        return !End_;
    }

    void* GetIP()
    {
        return IP_;
    }

    void* GetStartIP()
    {
        return StartIP_;
    }

private:
    unw_context_t Context_;
    unw_cursor_t Cursor_;
    bool End_ = false;
    int StepReturnValue_;

    void* StartIP_ = nullptr;
    void* IP_ = nullptr;

    void ReadIP()
    {
        unw_word_t ip = 0;
        int rv = unw_get_reg(&Cursor_, UNW_REG_IP, &ip);
        if (rv < 0) {
            End_ = true;
            return;
        }

        IP_ = reinterpret_cast<void*>(ip);

        unw_proc_info_t procInfo;
        if (unw_get_proc_info(&Cursor_, &procInfo) < 0) {
            StartIP_ = nullptr;
        } else {
            StartIP_ = reinterpret_cast<void*>(procInfo.start_ip);
        }
    }
};


void TCpuProfiler::OnSigProf(siginfo_t* info, ucontext_t* ucontext)
{
    SignalOverruns_ += info->si_overrun;

    // Skip signal handler frames.
    void* userIP = reinterpret_cast<void *>(ucontext->uc_mcontext.gregs[REG_RIP]);

    // For some reason, libunwind segfaults inside vDSO.
    if (VDSORange_.first <= userIP && userIP < VDSORange_.second) {
        return;
    }

    TUWCursor cursor;
    while (cursor.GetIP() != userIP) {
        if (!cursor.Next()) {
            return;
        }
    }

    int count = 0;
    bool startIP = true;
    auto ok = Queue_.TryPush([&] () -> std::pair<void*, bool> {
        if (count > Options_.MaxBacktraceSize) {
            return {nullptr, false};
        }

        if (startIP) {
            startIP = false;
            return {cursor.GetStartIP(), !cursor.IsEnd()};
        } else {
            startIP = true;
            count++;

            auto ip = cursor.GetIP();
            cursor.Next();
            return {ip, true};
        }
    });

    if (!ok) {
        QueueOverflows_++;
    }
}

void TCpuProfiler::DequeueSamples()
{
    TCpuSample sample;
    while (!Stop_) {
        Sleep(Options_.DequeuePeriod);

        while (true) {
            sample.Backtrace.clear();

            bool startIP = true;
            bool ok = Queue_.TryPop([&] (void* ip) {
                if (startIP) {
                    sample.Backtrace.push_back({ip, nullptr});
                    startIP = false;
                } else {
                    sample.Backtrace.back().second = ip;
                    startIP = true;
                }
            });

            if (!ok) {
                break;
            }

            Counters_[sample]++;
        }
    }
}

Profile TCpuProfiler::ReadProfile()
{
    Profile profile;
    profile.add_string_table();

    profile.add_string_table("cpu");
    profile.add_string_table("microseconds");
    profile.add_string_table("sample");
    profile.add_string_table("count");

    auto sampleType = profile.add_sample_type();
    sampleType->set_type(3);
    sampleType->set_unit(4);

    sampleType = profile.add_sample_type();
    sampleType->set_type(1);
    sampleType->set_unit(2);

    auto periodType = profile.mutable_period_type();
    periodType->set_type(1);
    periodType->set_unit(2);

    profile.set_period(1000000 / Options_.SamplingFrequency);

    THashMap<void*, ui64> locations;
    THashMap<void*, ui64> functions;

    for (const auto& [backtrace, count] : Counters_) {
        auto sample = profile.add_sample();
        sample->add_value(count);
        sample->add_value(count * profile.period());

        for (auto [startIP, ip] : backtrace.Backtrace) {
            auto it = locations.find(ip);
            if (it != locations.end()) {
                sample->add_location_id(it->second);
                continue;
            }

            auto locationId = locations.size() + 1;

            auto location = profile.add_location();
            location->set_address(reinterpret_cast<ui64>(ip));
            location->set_id(locationId);

            if (startIP != nullptr) {
                auto functionIt = functions.find(startIP);
                if (functionIt != functions.end()) {
                    auto line = location->add_line();
                    line->set_function_id(functionIt->second);
                } else {
                    auto fn = profile.add_function();
                    fn->set_id(reinterpret_cast<ui64>(startIP));
                    functions[startIP] = fn->id();

                    auto line = location->add_line();
                    line->set_function_id(fn->id());
                }
            }

            sample->add_location_id(locationId);
            locations[ip] = locationId;
        }
    }

    return profile;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProf
