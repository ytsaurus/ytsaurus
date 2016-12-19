#include "core_dumper.h"

#include "config.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/contrib/coredumper/coredumper.h>

#include <util/system/getpid.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void RedirectCoreDumpToFile(
    TFileInput&& coreInput,
    TFileOutput&& coreOutput)
{
    TransferData(&coreInput, &coreOutput);
}

////////////////////////////////////////////////////////////////////////////////

TCoreDumper::TCoreDumper(const TCoreDumperConfigPtr& config)
    : Config_(config)
    , ActionQueue_(New<TActionQueue>("CoreDumper"))
{ }

TCoreDump TCoreDumper::WriteCoreDump(const std::vector<Stroka>& notes)
{
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
            THROW_ERROR_EXCEPTION("Error while creating core dump")
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
    TFileInput coreInput(coreInputFile);
    TFileOutput coreOutput(corePath);

    auto asyncResult = BIND(RedirectCoreDumpToFile, Passed(std::move(coreInput)), Passed(std::move(coreOutput)))
        .AsyncVia(ActionQueue_->GetInvoker())
        .Run();

    return {corePath, asyncResult};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
