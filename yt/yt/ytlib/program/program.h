#pragma once

#include <yt/yt/core/misc/public.h>

#include <library/cpp/getopt/last_getopt.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProgramExitCode,
    ((OK)(0))
    ((OptionsError)(1))
    ((ProgramError)(2))
);

class TProgram
{
public:
    explicit TProgram(bool suppressVersion = false);
    ~TProgram();

    TProgram(const TProgram&) = delete;
    TProgram(TProgram&&) = delete;

    int Run(int argc, const char** argv);

    //! Handle --version/--yt-version/--build [--yson] if they are present.
    void HandleVersionAndBuild() const;

protected:
    NLastGetopt::TOpts Opts_;
    TString Argv0_;
    bool PrintVersion_ = false;
    bool PrintBuild_ = false;
    bool UseYson_ = false;

    [[noreturn]] int Exit(EProgramExitCode code) const noexcept;
    [[noreturn]] int Exit(int code) const noexcept;

    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) = 0;

    virtual void OnError(const TString& message) const noexcept;

    void SetCrashOnError();

    virtual void PrintVersionAndExit() const;
    virtual void PrintBuildAndExit() const;

private:
    bool CrashOnError_ = false;

    // Custom handler for option parsing errors.
    class TOptsParseResult;
};

//! The simplest exception possible.
//! Here we refrain from using TErrorException, as it relies on proper configuration of singleton subsystems,
//! which might not be the case during startup.
class TProgramException
    : public std::exception
{
public:
    explicit TProgramException(TString what);

    virtual const char* what() const noexcept override;

private:
    const TString What_;
};

////////////////////////////////////////////////////////////////////////////////

//! Helper for TOpt::StoreMappedResult to validate file paths for existance.
TString CheckPathExistsArgMapper(const TString& arg);

//! Helper for TOpt::StoreMappedResult to parse guids.
TGuid CheckGuidArgMapper(const TString& arg);

//! Drop privileges and save them if running with suid-bit.
void ConfigureUids();

void ConfigureIgnoreSigpipe();

//! Intercepts standard crash signals (see signal_registry.h for full list) with a nice handler.
void ConfigureCrashHandler();

//! Intercepts SIGTERM and terminates the process immediately with zero exit code.
void ConfigureExitZeroOnSigterm();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
