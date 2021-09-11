#include "cpu_profiler.h"
#include "symbolize.h"
#include "backtrace.h"

#if defined(_linux_)
#include <link.h>
#include <sys/syscall.h>

#include <util/system/yield.h>

#include <csignal>

#include <util/generic/yexception.h>
#endif

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

struct TCpuTagImpl
{
    TString Name;
    std::optional<TString> StringValue;
    std::optional<ui64> IntValue;
};

TCpuProfilerTag NewStringTag(const TString& name, const TString& value)
{
    auto tag = new TCpuTagImpl{
        .Name = name,
        .StringValue = value,
    };

    return reinterpret_cast<TCpuProfilerTag>(tag);
}

TCpuProfilerTag NewIntTag(const TString& name, const ui64 value)
{
    auto tag = new TCpuTagImpl{
        .Name = name,
        .IntValue = value,
    };

    return reinterpret_cast<TCpuProfilerTag>(tag);
}

thread_local std::array<volatile TCpuProfilerTag, MaxActiveTags> CpuProfilerTags;

////////////////////////////////////////////////////////////////////////////////

TCpuSample::operator size_t() const
{
    size_t hash = Tid;
    for (auto [startIP, ip] : Backtrace) {
        hash ^= reinterpret_cast<size_t>(startIP);
        hash = (hash << 1) | (hash >> 63);

        hash ^= reinterpret_cast<size_t>(ip);
        hash = (hash << 1) | (hash >> 63);
    }

    for (auto tag : Tags) {
        hash ^= reinterpret_cast<size_t>(tag);
        hash = (hash << 1) | (hash >> 63);
    }

    return hash;
}

////////////////////////////////////////////////////////////////////////////////

#if not defined(_linux_)

TCpuProfiler::TCpuProfiler(TCpuProfilerOptions options)
{
    Y_UNUSED(options);
}

TCpuProfiler::~TCpuProfiler()
{ }

void TCpuProfiler::Start()
{ }

void TCpuProfiler::Stop()
{ }

NProto::Profile TCpuProfiler::ReadProfile()
{
    return {};
}

#endif

////////////////////////////////////////////////////////////////////////////////

#if defined(_linux_)

size_t GetTid()
{
    return static_cast<size_t>(::syscall(SYS_gettid));
}

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
    bool pushTid = false;
    int tagIndex = 0;

    auto ok = Queue_.TryPush([&] () -> std::pair<void*, bool> {
        if (!pushTid) {
            pushTid = true;
            return {reinterpret_cast<void*>(GetTid()), true};
        }

        if (tagIndex < MaxActiveTags) {
            auto tag = CpuProfilerTags[tagIndex];
            tagIndex++;
            return {reinterpret_cast<void*>(tag), true};
        }

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
            sample.Tags.clear();

            std::optional<size_t> tid;

            bool startIP = true;
            int tagIndex = 0;
            bool ok = Queue_.TryPop([&] (void* ip) {
                if (!tid) {
                    tid = reinterpret_cast<size_t>(ip);
                    return;
                }

                if (tagIndex < MaxActiveTags) {
                    auto tag = reinterpret_cast<TCpuTagImpl*>(ip);
                    if (tag) {
                        sample.Tags.push_back(tag);
                    }
                    tagIndex++;
                    return;
                }

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

            sample.Tid = *tid;
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
    profile.add_string_table("tid");

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

    THashMap<TCpuTagImpl*, ui64> tagNames;
    THashMap<TCpuTagImpl*, ui64> tagValues;

    THashMap<void*, ui64> locations;
    THashMap<void*, ui64> functions;

    for (const auto& [backtrace, count] : Counters_) {
        auto sample = profile.add_sample();
        sample->add_value(count);
        sample->add_value(count * profile.period());

        auto label = sample->add_label();
        label->set_key(5); // tid
        label->set_num(backtrace.Tid);

        for (auto tag : backtrace.Tags) {
            auto label = sample->add_label();

            if (auto it = tagNames.find(tag); it != tagNames.end()) {
                label->set_key(it->second);
            } else {
                auto nameId = profile.string_table_size();
                profile.add_string_table(tag->Name);
                tagNames[tag] = nameId;
                label->set_key(nameId);
            }

            if (tag->IntValue) {
                label->set_num(*tag->IntValue);
            }

            if (tag->StringValue) {
                if (auto it = tagValues.find(tag); it != tagValues.end()) {
                    label->set_str(it->second);
                } else {
                    auto valueId = profile.string_table_size();
                    profile.add_string_table(*tag->StringValue);
                    tagValues[tag] = valueId;
                    label->set_key(valueId);
                }
            }
        }

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

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
