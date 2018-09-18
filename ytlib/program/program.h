#pragma once

#include <yt/core/misc/public.h>

#include <library/getopt/last_getopt.h>

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
    TProgram();
    ~TProgram();

    TProgram(const TProgram&) = delete;
    TProgram(TProgram&&) = delete;

    int Run(int argc, const char** argv);

protected:
    NLastGetopt::TOpts Opts_;
    TString Argv0_;

    int Exit(EProgramExitCode code) const noexcept;
    int Exit(int code) const noexcept;

    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) = 0;

    virtual void OnError(const TString& message) const noexcept;

    void SetCrashOnError();

private:
    bool CrashOnError_ = false;

    // Custom handler for option parsing errors.
    class TOptsParseResult;

    void PrintVersionAndExit();
    void PrintBuildAndExit();
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

//! Blocks SIGPIPE and masks SIGHUP.
void ConfigureSignals();

//! Intercepts SIGSEGV with a nice handler.
void ConfigureCrashHandler();

void ConfigureExitZeroOnSigterm();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
