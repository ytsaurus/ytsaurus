#include "cpu_profiler.h"
#include "symbolize.h"
#include "backtrace.h"

#include <link.h>

#include <util/system/yield.h>

#include <csignal>

#include <util/generic/yexception.h>

namespace NYT::NYTProf {

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

TCpuProfiler::TCpuProfiler(TCpuProfilerOptions options)
    : Options_(options)
    , Queue_(options.RingBufferLogSize)
{
    VdsoRange_ = GetVdsoRange();
}

TCpuProfiler::~TCpuProfiler()
{
    Stop();
}

void TCpuProfiler::Start()
{
    StartTimer();

    Stop_ = false;
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
    TCpuProfiler* expected = nullptr;
    if (!ActiveProfiler_.compare_exchange_strong(expected, this)) {
        throw yexception() << "another instance of CPU profiler is running";
    }

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

void TCpuProfiler::OnSigProf(siginfo_t* info, ucontext_t* ucontext)
{
    SignalOverruns_ += info->si_overrun;

    // Skip signal handler frames.
    void* userIP = reinterpret_cast<void *>(ucontext->uc_mcontext.gregs[REG_RIP]);

    // For some reason, libunwind segfaults inside vDSO.
    if (VdsoRange_.first <= userIP && userIP < VdsoRange_.second) {
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

            auto ip = cursor.GetIP();

            if (count != 0) {
                // First IP points to next executing instruction.
                // All other IP's are return addresses.
                // Substract 1 to get accurate line information for profiler.
                ip = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(ip) - 1);
            }

            cursor.Next();
            count++;

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

NProto::Profile TCpuProfiler::ReadProfile()
{
    NProto::Profile profile;
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

} // namespace NYT::NYTProf
