#include "subprocess.h"
#include "finally.h"
#include "proc.h"

#include <yt/core/logging/log.h>

#include <yt/core/net/connection.h>

#include <util/system/execpath.h>

#include <array>

namespace NYT {

using namespace NConcurrency;
using namespace NPipes;

////////////////////////////////////////////////////////////////////////////////

const static size_t PipeBlockSize = 64 * 1024;
static NLogging::TLogger Logger("Subprocess");

////////////////////////////////////////////////////////////////////////////////

TSubprocess::TSubprocess(const TString& path, bool copyEnv)
    : Process_(New<TSimpleProcess>(path, copyEnv))
{ }

TSubprocess TSubprocess::CreateCurrentProcessSpawner()
{
    return TSubprocess(GetExecPath());
}

void TSubprocess::AddArgument(TStringBuf arg)
{
    Process_->AddArgument(arg);
}

void TSubprocess::AddArguments(std::initializer_list<TStringBuf> args)
{
    Process_->AddArguments(args);
}

TSubprocessResult TSubprocess::Execute(const TSharedRef& input)
{
#ifdef _unix_
    auto inputStream = Process_->GetStdInWriter();
    auto outputStream = Process_->GetStdOutReader();
    auto errorStream = Process_->GetStdErrReader();
    auto finished = Process_->Spawn();

    auto readIntoBlob = [] (IAsyncInputStreamPtr stream) {
        TBlob output;
        auto buffer = TSharedMutableRef::Allocate(PipeBlockSize, false);
        while (true) {
            auto size = WaitFor(stream->Read(buffer))
                .ValueOrThrow();

            if (size == 0)
                break;

            // ToDo(psushin): eliminate copying.
            output.Append(buffer.Begin(), size);
        }
        return TSharedRef::FromBlob(std::move(output));
    };

    auto writeStdin = BIND([=] {
        if (input.Size() > 0) {
            WaitFor(inputStream->Write(input))
                .ThrowOnError();
        }

        WaitFor(inputStream->Close())
            .ThrowOnError();

        //! Return dummy ref, so later we cat put Future into vector
        //! along with stdout and stderr.
        return EmptySharedRef;
    });

    std::vector<TFuture<TSharedRef>> futures = {
        BIND(readIntoBlob, outputStream).AsyncVia(GetCurrentInvoker()).Run(),
        BIND(readIntoBlob, errorStream).AsyncVia(GetCurrentInvoker()).Run(),
        writeStdin.AsyncVia(GetCurrentInvoker()).Run(),
    };

    try {
        auto outputsOrError = WaitFor(Combine(futures));
        THROW_ERROR_EXCEPTION_IF_FAILED(
            outputsOrError, 
            "IO error occurred during subprocess call");

        const auto& outputs = outputsOrError.Value();
        YCHECK(outputs.size() == 3);

        // This can block indefinitely.
        auto exitCode = WaitFor(finished);
        return TSubprocessResult{outputs[0], outputs[1], exitCode};
    } catch (...) {
        try {
            Process_->Kill(SIGKILL);
        } catch (...) { }
        Y_UNUSED(WaitFor(finished));
        throw;
    }
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSubprocess::Kill(int signal)
{
    Process_->Kill(signal);
}

TString TSubprocess::GetCommandLine() const
{
    return Process_->GetCommandLine();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
