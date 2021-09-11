#include "program.h"

#include "build_attributes.h"

#include <yt/yt/build/build.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/signal_registry.h>
#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/library/ytprof/heap_profiler.h>

#include <yt/yt/library/mlock/mlock.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <tcmalloc/malloc_extension.h>

#include <absl/debugging/stacktrace.h>

#include <util/system/thread.h>
#include <util/system/sigset.h>

#include <util/string/subst.h>

#include <thread>

#ifdef _unix_
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#endif

#ifdef _linux_
#include <grp.h>
#include <sys/prctl.h>
#endif

#if defined(_linux_) && defined(CLANG_COVERAGE)
extern "C" int __llvm_profile_write_file(void);
extern "C" void __llvm_profile_set_filename(const char* name);
#endif

namespace NYT {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TProgram::TOptsParseResult
    : public NLastGetopt::TOptsParseResult
{
public:
    TOptsParseResult(TProgram* owner, int argc, const char** argv)
        : Owner_(owner)
    {
        Init(&Owner_->Opts_, argc, argv);
    }

    void HandleError() const override
    {
        Owner_->OnError(CurrentExceptionMessage());
        Cerr << Endl << "Try running '" << Owner_->Argv0_ << " --help' for more information." << Endl;
        Owner_->Exit(EProgramExitCode::OptionsError);
    }

private:
    TProgram* Owner_;
};

TProgram::TProgram()
{
    Opts_.AddHelpOption();
    Opts_.AddLongOption("yt-version", "print YT version and exit")
        .NoArgument()
        .StoreValue(&PrintYTVersion_, true);
    Opts_.AddLongOption("version", "print version and exit")
        .NoArgument()
        .StoreValue(&PrintVersion_, true);
    Opts_.AddLongOption("yson", "print build information in YSON")
        .NoArgument()
        .StoreValue(&UseYson_, true);
    Opts_.AddLongOption("build", "print build information and exit")
        .NoArgument()
        .StoreValue(&PrintBuild_, true);
    Opts_.SetFreeArgsNum(0);

    ConfigureCoverageOutput();
}

void TProgram::SetCrashOnError()
{
    CrashOnError_ = true;
}

TProgram::~TProgram() = default;

void TProgram::HandleVersionAndBuild() const
{
    if (PrintVersion_) {
        PrintVersionAndExit();
        Y_UNREACHABLE();
    }
    if (PrintYTVersion_) {
        PrintYTVersionAndExit();
        Y_UNREACHABLE();
    }
    if (PrintBuild_) {
        PrintBuildAndExit();
        Y_UNREACHABLE();
    }
}

int TProgram::Run(int argc, const char** argv)
{
    TThread::SetCurrentThreadName("ProgramMain");

    srand(time(nullptr));

    auto run = [&] {
        Argv0_ = TString(argv[0]);
        TOptsParseResult result(this, argc, argv);

        HandleVersionAndBuild();

        DoRun(result);
    };

    if (!CrashOnError_) {
        try {
            run();
            return Exit(EProgramExitCode::OK);
        } catch (...) {
            OnError(CurrentExceptionMessage());
            return Exit(EProgramExitCode::ProgramError);
        }
    } else {
        run();
        return Exit(EProgramExitCode::OK);
    }
}

int TProgram::Exit(EProgramExitCode code) const noexcept
{
    Exit(static_cast<int>(code));
}

int TProgram::Exit(int code) const noexcept
{
    NLogging::TLogManager::StaticShutdown();

#if defined(_linux_) && defined(CLANG_COVERAGE)
    __llvm_profile_write_file();
#endif

    // No graceful shutdown at the moment.
    _exit(code);

    YT_ABORT();
}

void TProgram::OnError(const TString& message) const noexcept
{
    try {
        Cerr << message << Endl;
    } catch (...) {
        // Just ignore it; STDERR might be closed already,
        // and write() would result in EPIPE.
    }
}

void TProgram::PrintYTVersionAndExit() const
{
    if (UseYson_) {
        THROW_ERROR_EXCEPTION("--yson is not supported when printing version");
    }
    Cout << GetVersion() << Endl;
    _exit(0);
}

void TProgram::PrintBuildAndExit() const
{
    if (UseYson_) {
        TYsonWriter writer(&Cout, EYsonFormat::Pretty);
        BuildBuildAttributes(&writer);
        Cout << Endl;
    } else {
        Cout << "Build Time: " << GetBuildTime() << Endl;
        Cout << "Build Host: " << GetBuildHost() << Endl;
    }
    _exit(0);
}

void TProgram::PrintVersionAndExit() const
{
    PrintYTVersionAndExit();
}

////////////////////////////////////////////////////////////////////////////////

TProgramException::TProgramException(TString what)
    : What_(std::move(what))
{ }

const char* TProgramException::what() const noexcept
{
    return What_.c_str();
}

////////////////////////////////////////////////////////////////////////////////

TString CheckPathExistsArgMapper(const TString& arg)
{
    if (!NFS::Exists(arg)) {
        throw TProgramException(Format("File %v does not exist", arg));
    }
    return arg;
}

TGuid CheckGuidArgMapper(const TString& arg)
{
    TGuid result;
    if (!TGuid::FromString(arg, &result)) {
        throw TProgramException(Format("Error parsing guid %Qv", arg));
    }
    return result;
}

void ConfigureUids()
{
#ifdef _unix_
    uid_t ruid, euid;
#ifdef _linux_
    uid_t suid;
    YT_VERIFY(getresuid(&ruid, &euid, &suid) == 0);
#else
    ruid = getuid();
    euid = geteuid();
#endif
    if (euid == 0) {
        // if real uid is already root do not set root as supplementary ids.
        if (ruid != 0) {
            YT_VERIFY(setgroups(0, nullptr) == 0);
        }
        // if effective uid == 0 (e. g. set-uid-root), alter saved = effective, effective = real.
#ifdef _linux_
        YT_VERIFY(setresuid(ruid, ruid, euid) == 0);
        // Make server suid_dumpable = 1.
        YT_VERIFY(prctl(PR_SET_DUMPABLE, 1)  == 0);
#else
        YT_VERIFY(setuid(euid) == 0);
        YT_VERIFY(seteuid(ruid) == 0);
        YT_VERIFY(setruid(ruid) == 0);
#endif
    }
    umask(0000);
#endif
}

void ConfigureCoverageOutput()
{
#if defined(_linux_) && defined(CLANG_COVERAGE)
    // YT tests use pid namespaces. We can't use process id as unique identifier for output file.
    if (auto profileFile = getenv("LLVM_PROFILE_FILE")) {
        TString fixedProfile{profileFile};
        SubstGlobal(fixedProfile, "%e", "ytserver-all");
        SubstGlobal(fixedProfile, "%p", ToString(TInstant::Now().NanoSeconds()));
        __llvm_profile_set_filename(fixedProfile.c_str());
    }
#endif
}

void ConfigureIgnoreSigpipe()
{
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
}

void ConfigureCrashHandler()
{
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
    TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);
}

void ExitZero(int /* unused */)
{
#if defined(_linux_) && defined(CLANG_COVERAGE)
    __llvm_profile_write_file();
#endif
    _exit(0);
}

void ConfigureExitZeroOnSigterm()
{
#ifdef _unix_
    signal(SIGTERM, ExitZero);
#endif
}

void ConfigureAllocator(TAllocatorOptions options)
{
    NYT::MlockFileMappings();

#ifdef _linux_
    NYTAlloc::EnableYTLogging();
    NYTAlloc::EnableYTProfiling();
    NYTAlloc::InitializeLibunwindInterop();
    NYTAlloc::SetEnableEagerMemoryRelease(options.YTAllocEagerMemoryRelease);
    if (options.YTAllocStockpile) {
        NYTAlloc::EnableStockpile();
    }

    if (tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes")) {
        std::thread backgroundThread([] {
            TThread::SetCurrentThreadName("TCAllocBack");
            tcmalloc::MallocExtension::ProcessBackgroundActions();
        });
        backgroundThread.detach();
    }

    absl::SetStackUnwinder(NYTProf::AbslStackUnwinder);
    // TODO(prime@): tune parameters.
    tcmalloc::MallocExtension::SetProfileSamplingRate(2_MB);
    tcmalloc::MallocExtension::SetGuardedSamplingRate(128_MB);
    tcmalloc::MallocExtension::ActivateGuardedSampling();
    tcmalloc::MallocExtension::SetMaxPerCpuCacheSize(3_MB);
    tcmalloc::MallocExtension::SetMaxTotalThreadCacheBytes(24_MB);
    tcmalloc::MallocExtension::SetBackgroundReleaseRate(tcmalloc::MallocExtension::BytesPerSecond{1_MB});
#else
    Y_UNUSED(options);
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
