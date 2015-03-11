#include "stdafx.h"

#include "subprocess.h"
#include "private.h"

#include <ytlib/pipes/async_reader.h>

#include <core/misc/proc.h>
#include <core/misc/finally.h>

#include <util/system/execpath.h>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;
using namespace NPipes;

////////////////////////////////////////////////////////////////////////////////

const static size_t PipeBlockSize = 64 * 1024;
const static auto& Logger = ExecAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TSubprocess::TSubprocess(const Stroka& path)
    : Process_(path)
{ }

TSubprocess TSubprocess::CreateCurrentProcessSpawner()
{
    return TSubprocess(GetExecPath());
}

void TSubprocess::AddArgument(TStringBuf arg)
{
    Process_.AddArgument(arg);
}

void TSubprocess::AddArguments(std::initializer_list<TStringBuf> args)
{
    Process_.AddArguments(args);
}

TSubprocessResult TSubprocess::Execute()
{
    std::array<TPipe, 3> pipes;

    {
        TPipeFactory pipeFactory(3);
        for (auto& pipe: pipes) {
            pipe = pipeFactory.Create();
        }
        pipeFactory.Clear();
    }

    for (int index = 0; index < pipes.size(); ++index) {
        const auto& pipe = pipes[index];

        auto FD = index == 0 ? pipe.ReadFD : pipe.WriteFD;
        Process_.AddDup2FileAction(FD, index);
    }

    TFinallyGuard finallyGuard([&] () {
        if (Process_.Started() && !Process_.Finished()) {
            try {
                Process_.Kill(9);
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Cannot kill subprocess %v", Process_.GetProcessId());
            }

            auto error = Process_.Wait();
            if (!error.IsOK()) {
                LOG_ERROR(error, "Cannot wait subprocess %v", Process_.GetProcessId());
            }
        }
    });

    try {
        Process_.Spawn();

        for (int index = 0; index < pipes.size(); ++index) {
            auto& pipe = pipes[index];

            SafeClose(pipe.WriteFD);
            pipe.WriteFD = -1;
            if (index == 0) {
                SafeClose(pipe.ReadFD);
                pipe.ReadFD = -1;
            } else {
                SafeMakeNonblocking(pipe.ReadFD);
            }
        }
    } catch (const std::exception& ) {
        for (const auto& pipe : pipes) {
            TryClose(pipe.ReadFD);
            TryClose(pipe.WriteFD);
        }

        throw;
    }

    std::array<IAsyncZeroCopyInputStreamPtr, 2> processOutputs = {
        CreateZeroCopyAdapter(New<TAsyncReader>(pipes[1].ReadFD), PipeBlockSize),
        CreateZeroCopyAdapter(New<TAsyncReader>(pipes[2].ReadFD), PipeBlockSize)
    };
    std::vector<TFuture<TSharedRef>> futures(2);

    for (int i = 0; i < processOutputs.size(); ++i) {
        auto processOutput = processOutputs[i];
        futures[i] = BIND([processOutput] () {
            TBlob output;
            while (true) {
                auto block = WaitFor(processOutput->Read())
                    .ValueOrThrow();

                if (!block)
                    break;

                output.Append(block);
            }
            return TSharedRef::FromBlob(std::move(output));
        }).AsyncVia(GetCurrentInvoker()).Run();
    }

    auto outputsOrError = WaitFor(Combine(futures));
    if (!outputsOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("IO error occured during subprocess call")
            << outputsOrError;
    }

    const auto& outputs = outputsOrError.Value();
    YCHECK(outputs.size() == 2);

    // This can block indefinetely.
    auto exitCode = Process_.Wait();
    return TSubprocessResult{outputs[0], outputs[1], exitCode};
}

void TSubprocess::Kill(int signal)
{
    Process_.Kill(signal);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
