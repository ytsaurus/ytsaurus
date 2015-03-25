#include "stdafx.h"

#include "subprocess.h"

#include "proc.h"
#include "finally.h"

#include <core/pipes/async_reader.h>

#include <core/logging/log.h>

#include <util/system/execpath.h>

#include <array>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;
using namespace NPipes;

////////////////////////////////////////////////////////////////////////////////

const static size_t PipeBlockSize = 64 * 1024;
static NLogging::TLogger Logger("Subprocess");

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
#ifndef _win_
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
                LOG_ERROR(ex, "Failed to kill subprocess %v", Process_.GetProcessId());
            }

            auto error = Process_.Wait();
            if (!error.IsOK()) {
                LOG_ERROR(error, "Failed to wait subprocess %v", Process_.GetProcessId());
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
    THROW_ERROR_EXCEPTION_IF_FAILED(outputsOrError, "IO error occured during subprocess call");

    const auto& outputs = outputsOrError.Value();
    YCHECK(outputs.size() == 2);

    // This can block indefinetely.
    auto exitCode = Process_.Wait();
    return TSubprocessResult{outputs[0], outputs[1], exitCode};
#else
    THROW_ERROR_EXCEPTION("Unsupported platform");
#endif
}

void TSubprocess::Kill(int signal)
{
    Process_.Kill(signal);
}

Stroka TSubprocess::GetCommandLine() const
{
    return Process_.GetCommandLine();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
