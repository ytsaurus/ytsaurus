#include "core_dumper.h"
#include "private.h"
#include "config.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/core_dumper.h>

#include <util/system/mutex.h>

#ifdef _linux_
    #include <yt/contrib/coredumper/coredumper.h>
#endif

#include <util/system/getpid.h>

namespace NYT::NCoreDump {

using namespace NConcurrency;

static const auto& Logger = CoreDumpLogger;

////////////////////////////////////////////////////////////////////////////////

class TCoreDumper
    : public ICoreDumper
{
public:
    explicit TCoreDumper(TCoreDumperConfigPtr config)
        : Config_(std::move(config))
    { }

    virtual TCoreDump WriteCoreDump(const std::vector<TString>& notes) override
    {
        auto id = TGuid::Create();
        YT_LOG_INFO("Writing core dump (Id: %v, Notes: %v)", id, notes);
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

        // Value of 42 below is a mock for the %s (signal) component of core pattern.
        auto corePath = Format("%v/core.%v.42.%v.%v_coredumper",
            Config_->Path,
            GetPID(),
            TInstant::Now().TimeT(),
            Config_->ComponentName);

        TFile coreInputFile = TFile(fd);
        TUnbufferedFileInput coreInput(coreInputFile);
        TUnbufferedFileOutput coreOutput(corePath);

        auto asyncResult = BIND(RedirectCoreDumpToFile, Passed(std::move(coreInput)), Passed(std::move(coreOutput)), id)
            .AsyncVia(ActionQueue_->GetInvoker())
            .Run();

        return {corePath, asyncResult};
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Error creating core dump (Id: %v)", id);
        THROW_ERROR_EXCEPTION("Error creating core dump")
            << ex;
    }
#else
        THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
    }

private:
    const TCoreDumperConfigPtr Config_;
    const NConcurrency::TActionQueuePtr ActionQueue_ = New<TActionQueue>("CoreDumper");

    TMutex Mutex_;

private:
    static void RedirectCoreDumpToFile(
        TUnbufferedFileInput&& coreInput,
        TUnbufferedFileOutput&& coreOutput,
        TGuid id)
    {
        YT_LOG_INFO("Started transferring core dump data (Id: %v)", id);
        auto size = TransferData(&coreInput, &coreOutput);
        YT_LOG_INFO("Finished transferring core dump data (Id: %v, Size: %v)", id, size);
    }
};

ICoreDumperPtr CreateCoreDumper(TCoreDumperConfigPtr config)
{
    return New<TCoreDumper>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump
