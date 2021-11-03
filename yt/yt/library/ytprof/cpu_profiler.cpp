#include "cpu_profiler.h"
#include "symbolize.h"
#include "backtrace.h"

#if defined(_linux_)
#include <link.h>
#include <sys/syscall.h>
#include <sys/prctl.h>

#include <util/system/yield.h>

#include <csignal>
#include <functional>

#include <util/generic/yexception.h>
#endif

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TProfilerTag)

struct TCpuProfilerTags;

// This variable is referenced from signal handler.
constinit thread_local std::atomic<TCpuProfilerTags*> CpuProfilerTagsPtr = nullptr;

struct TCpuProfilerTags
{
    TCpuProfilerTags()
    {
        CpuProfilerTagsPtr = this;
    }

    ~TCpuProfilerTags()
    {
        CpuProfilerTagsPtr = nullptr;
    }

    std::array<TAtomicSignalPtr<TProfilerTag>, MaxActiveTags> Tags;
};

// We can't reference CpuProfilerTags from signal handler,
// since it may trigger lazy initialization.
thread_local TCpuProfilerTags CpuProfilerTags;

////////////////////////////////////////////////////////////////////////////////

TCpuProfilerTagGuard::TCpuProfilerTagGuard(TProfilerTagPtr tag)
{
    for (int i = 0; i < MaxActiveTags; i++) {
        if (!CpuProfilerTags.Tags[i].IsSetFromThread()) {
            CpuProfilerTags.Tags[i].StoreFromThread(std::move(tag));
            TagIndex_ = i;
            return;
        }
    }
}

TCpuProfilerTagGuard::~TCpuProfilerTagGuard()
{
    if (TagIndex_ != -1) {
        CpuProfilerTags.Tags[TagIndex_].StoreFromThread(nullptr);
    }
}

TCpuProfilerTagGuard::TCpuProfilerTagGuard(TCpuProfilerTagGuard&& other)
    : TagIndex_(other.TagIndex_)
{
    other.TagIndex_ = -1;
}

TCpuProfilerTagGuard& TCpuProfilerTagGuard::operator = (TCpuProfilerTagGuard&& other)
{
    if (TagIndex_ != -1) {
        CpuProfilerTags.Tags[TagIndex_].StoreFromThread(nullptr);
    }

    other.TagIndex_ = -1;
    return *this;
}

////////////////////////////////////////////////////////////////////////////////

TCpuSample::operator size_t() const
{
    size_t hash = Tid;
    hash = CombineHashes(hash, std::hash<TString>()(ThreadName));

    for (auto ip : Backtrace) {
        hash = CombineHashes(hash, ip);
    }

    for (const auto& tag : Tags) {
        hash = CombineHashes(hash,
            CombineHashes(
                std::hash<TString>{}(tag.first),
                std::hash<std::variant<TString, i64>>{}(tag.second)));
    }

    return hash;
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK void* AcquireFiberTagStorage()
{
    return nullptr;
}

Y_WEAK std::vector<std::pair<TString, std::variant<TString, i64>>> ReadFiberTags(void* /* storage */)
{
    return {};
}

Y_WEAK void ReleaseFiberTagStorage(void* /* storage */)
{ }

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
    if (!IsProfileBuild()) {
        throw yexception() << "frame pointers not available; rebuild with --build=profile";
    }

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

    void* rip = reinterpret_cast<void *>(ucontext->uc_mcontext.gregs[REG_RIP]);
    void* rsp = reinterpret_cast<void *>(ucontext->uc_mcontext.gregs[REG_RSP]);
    void* rbp = reinterpret_cast<void *>(ucontext->uc_mcontext.gregs[REG_RBP]);

    TFramePointerCursor cursor(&Mem_, rip, rsp, rbp);

    int count = 0;
    bool pushTid = false;
    bool pushFiberStorage = false;
    int tagIndex = 0;

    auto tagsPtr = CpuProfilerTagsPtr.load();

    uintptr_t threadName[2] = {};
    prctl(PR_GET_NAME, (unsigned long)threadName, 0UL, 0UL, 0UL);
    int namePushed = 0;

    auto ok = Queue_.TryPush([&] () -> std::pair<void*, bool> {
        if (!pushTid) {
            pushTid = true;
            return {reinterpret_cast<void*>(GetTid()), true};
        }

        if (namePushed < 2) {
            return {reinterpret_cast<void*>(threadName[namePushed++]), true};
        }

        if (!pushFiberStorage) {
            pushFiberStorage = true;
            return {AcquireFiberTagStorage(), true};
        }

        if (tagIndex < MaxActiveTags) {
            if (tagsPtr) {
                auto tag = tagsPtr->Tags[tagIndex].GetFromSignal();
                tagIndex++;
                return {reinterpret_cast<void*>(tag.Release()), true};
            } else {
                tagIndex++;
                return {nullptr, true};
            }
        }

        if (count > Options_.MaxBacktraceSize) {
            return {nullptr, false};
        }

        if (cursor.IsEnd()) {
            return {nullptr, false};
        }

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
            uintptr_t threadName[2] = {};
            int namePopped = 0;
            std::optional<void*> fiberStorage;

            int tagIndex = 0;
            bool ok = Queue_.TryPop([&] (void* ip) {
                if (!tid) {
                    tid = reinterpret_cast<size_t>(ip);
                    return;
                }

                if (namePopped < 2) {
                    threadName[namePopped++] = reinterpret_cast<uintptr_t>(ip);
                    return;
                }

                if (!fiberStorage) {
                    fiberStorage = ip;
                    return;
                }

                if (tagIndex < MaxActiveTags) {
                    auto tag = reinterpret_cast<TProfilerTag*>(ip);
                    if (tag) {
                        if (tag->StringValue) {
                            sample.Tags.emplace_back(tag->Name, *tag->StringValue);
                        } else {
                            sample.Tags.emplace_back(tag->Name, *tag->IntValue);
                        }
                    }
                    tagIndex++;
                    return;
                }

                sample.Backtrace.push_back(reinterpret_cast<ui64>(ip));
            });

            if (!ok) {
                break;
            }

            sample.ThreadName = TString{reinterpret_cast<char*>(threadName)};
            sample.Tid = *tid;
            for (auto& tag : ReadFiberTags(*fiberStorage)) {
                sample.Tags.push_back(std::move(tag));
            }
            ReleaseFiberTagStorage(*fiberStorage);

            Counters_[sample]++;
        }
    }
}

NProto::Profile TCpuProfiler::ReadProfile()
{
    NProto::Profile profile;
    profile.add_string_table();

    THashMap<TString, ui64> stringTable;
    auto stringify = [&] (const TString& str) -> i64 {
        if (auto it = stringTable.find(str); it != stringTable.end()) {
            return it->second;
        } else {
            auto nameId = profile.string_table_size();
            profile.add_string_table(str);
            stringTable[str] = nameId;
            return nameId;
        }
    };

    auto sampleType = profile.add_sample_type();
    sampleType->set_type(stringify("sample"));
    sampleType->set_unit(stringify("count"));

    sampleType = profile.add_sample_type();
    sampleType->set_type(stringify("cpu"));
    sampleType->set_unit(stringify("microseconds"));

    auto periodType = profile.mutable_period_type();
    periodType->set_type(stringify("cpu"));
    periodType->set_unit(stringify("microseconds"));

    profile.set_period(1000000 / Options_.SamplingFrequency);

    THashMap<uintptr_t, ui64> locations;

    for (const auto& [backtrace, count] : Counters_) {
        auto sample = profile.add_sample();
        sample->add_value(count);
        sample->add_value(count * profile.period());

        auto label = sample->add_label();
        label->set_key(stringify("tid"));
        label->set_num(backtrace.Tid);

        label = sample->add_label();
        label->set_key(stringify("thread"));
        label->set_str(stringify(backtrace.ThreadName));

        for (auto tag : backtrace.Tags) {
            auto label = sample->add_label();
            label->set_key(stringify(tag.first));

            if (auto intValue = std::get_if<i64>(&tag.second)) {
                label->set_num(*intValue);
            } else if (auto strValue = std::get_if<TString>(&tag.second)) {
                label->set_str(stringify(*strValue));
            }
        }

        for (auto ip : backtrace.Backtrace) {
            auto it = locations.find(ip);
            if (it != locations.end()) {
                sample->add_location_id(it->second);
                continue;
            }

            auto locationId = locations.size() + 1;

            auto location = profile.add_location();
            location->set_address(ip);
            location->set_id(locationId);

            sample->add_location_id(locationId);
            locations[ip] = locationId;
        }
    }

    return profile;
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
