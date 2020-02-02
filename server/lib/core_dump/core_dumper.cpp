#include "core_dumper.h"
#include "helpers.h"
#include "private.h"
#include "config.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/finally.h>

#include <yt/core/ytree/ypath_service.h>
#include <yt/core/ytree/fluent.h>

#include <util/system/mutex.h>

#ifdef _linux_
    #include <yt/contrib/coredumper/coredumper.h>
    #include <sys/prctl.h>
#endif

#include <unistd.h>

namespace NYT::NCoreDump {

using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TCoreDumper
    : public ICoreDumper
{
public:
    explicit TCoreDumper(TCoreDumperConfigPtr config)
        : Config_(std::move(config))
        , OrchidService_(IYPathService::FromProducer(BIND(&TCoreDumper::BuildYson, MakeWeak(this))))
    {
#ifdef _linux_
        if (prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY) != 0) {
            const auto& Logger = CoreDumpLogger;
            YT_LOG_ERROR(TError::FromSystem(), "Failed to call prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY)");
        }
#endif
    }

    virtual TCoreDump WriteCoreDump(const std::vector<TString>& notes, const TString& reason) override
    {
        ++ActiveCoreDumpCount_;
        auto activeCoreDumpCountGuard = Finally([&] { --ActiveCoreDumpCount_; });

        int index = Index_++;

        auto Logger = TLogger(CoreDumpLogger)
            .AddTag("Index: %v", index);

        YT_LOG_INFO("Writing core dump (Notes: %v, Reason: %v)",
            notes,
            reason);

#ifdef _linux_
        try {
            CoreDumpParameters parameters;
            ClearCoreDumpParameters(&parameters);

            std::vector<CoredumperNote> coredumperNotes;
            for (const auto& note : notes) {
                coredumperNotes.emplace_back(CoredumperNote {
                    YTCoreNoteName.data(),
                    YTCoreNoteType,
                    static_cast<ui32>(note.size()),
                    static_cast<const void*>(note.data())
                });
            }
            SetCoreDumpNotes(&parameters, coredumperNotes.data(), coredumperNotes.size());

            int fd = -1;

            {
                TGuard<TMutex> guard(Mutex_);

                fd = GetCoreDumpWith(&parameters);
                if (fd == -1) {
                    THROW_ERROR_EXCEPTION("Failed to start creating core dump")
                        << TError::FromSystem();
                }
            }

            auto corePath = Format("%v/%v", Config_->Path, Config_->Pattern);

            std::vector<std::pair<TString, TString>> variables = {
                {"%CORE_PID", ToString(getpid())},
                {"%CORE_TID", ToString(GetCurrentThreadId())},
                {"%CORE_THREAD_NAME", TThread::CurrentThreadName()},
                {"%CORE_SIG", "42"}, // A mock for coredumper.
                {"%CORE_REASON", reason},
                {"%CORE_INDEX", ToString(index)},
                {"%CORE_DATETIME", TInstant::Now().FormatLocalTime("%Y%m%dT%H%M%S")},
            };

            YT_LOG_DEBUG("Core dump variables (Variables: %v)",
                variables);

            // Replace all occurrences of variables with their values.
            for (const auto& [variable, value] : variables) {
                for (int position = 0; (position = corePath.find(variable, position)) != TString::npos; ) {
                    corePath.replace(position, variable.length(), value);
                }
            }

            YT_LOG_INFO("Redirecting core dump to file (InputFd: %v, OutputPath: %v)",
                fd,
                corePath);

            TFile coreInputFile(fd);
            TUnbufferedFileInput coreInput(coreInputFile);
            TFile coreOutputFile(corePath, CreateNew | WrOnly | Seq | CloseOnExec);

            auto asyncResult = BIND([
                coreInput = std::move(coreInput),
                coreOutputFile = std::move(coreOutputFile),
                activeCoreDumpCountGuard = std::move(activeCoreDumpCountGuard),
                Logger] () mutable {
                    YT_LOG_INFO("Started transferring core dump data");
                    try {
                        auto size = WriteSparseCoreDump(&coreInput, &coreOutputFile);
                        YT_LOG_INFO("Finished transferring core dump data (Size: %v)", size);
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Error writing core dump");
                        throw;
                    }
                })
                .AsyncVia(ActionQueue_->GetInvoker())
                .Run();

            return {corePath, asyncResult};
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error creating core dump");
            THROW_ERROR_EXCEPTION("Error creating core dump")
                << ex;
        }
#else
        THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
    }

    virtual const IYPathServicePtr& CreateOrchidService() const override
    {
        return OrchidService_;
    }

private:
    const TCoreDumperConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_ = New<TActionQueue>("CoreDumper");

    IYPathServicePtr OrchidService_;

    TMutex Mutex_;
    std::atomic<int> Index_ = {0};
    std::atomic<int> ActiveCoreDumpCount_ = {0};

    void BuildYson(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        BuildYsonFluently(consumer).BeginMap()
            .Item("total_count").Value(Index_.load())
            .Item("active_count").Value(ActiveCoreDumpCount_.load())
        .EndMap();
    }
};

ICoreDumperPtr CreateCoreDumper(TCoreDumperConfigPtr config)
{
    return New<TCoreDumper>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
