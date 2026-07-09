#include "crash_recorder.h"

#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/core/net/local_address.h>

#include <library/cpp/yt/error/origin_attributes.h>

#include <library/cpp/yt/system/handle_eintr.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <util/generic/buffer.h>

#include <util/folder/dirut.h>

#include <util/stream/buffer.h>
#include <util/stream/file.h>

#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/fstat.h>

#include <filesystem>

namespace NYT::NFlow {

using namespace NNet;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

std::atomic<FHANDLE> ExtraCrashOutputFileHandle = INVALID_FHANDLE;

TAtomicObject<std::string> ProcessFingerprint;

std::string GetCurrentFileName(std::string processFingerprint)
{
    return std::string(Format("%v/ytflow-crashreport-actual-candidate-%v.txt", GetSystemTempDir(), processFingerprint));
}

std::string GetPreviousFileName(std::string processFingerprint)
{
    return std::string(Format("%v/ytflow-crashreport-previous-%v.txt", GetSystemTempDir(), processFingerprint));
}

void WriteToStderrAndExtraFd(TStringBuf str)
{
    WriteToStderr(str);

    auto extraFileHandle = ExtraCrashOutputFileHandle.load();
    if (extraFileHandle != INVALID_FHANDLE) {
        TFileHandle file(extraFileHandle);
        file.Write(str.data(), str.size()); // Ignore errors.
        file.Release();
    }
}

TError ReadCrashReport(std::string filePath)
{
    try {
        if (!std::filesystem::exists(filePath)) {
            return TError();
        }

        TFile file(filePath, EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
        TBufferOutput bufferOutput;
        TUnbufferedFileInput(file).ReadAll(bufferOutput);
        auto report = TStringBuf(bufferOutput.Buffer().data(), bufferOutput.Buffer().size());

        if (report.empty()) {
            return TError();
        }

        auto error = TError("Process crashed")
            << TErrorAttribute("host", GetLocalHostName())
            << TErrorAttribute("datetime", TInstant::Seconds(TFileStat(file).MTime))
            << TErrorAttribute("crash_report", report);
        error.UpdateOriginAttributes();
        return error;
    } catch (const TFileError& error) {
        WriteToStderr(Format("Can not open file %Qv with crash report; error: %v\n", filePath, LastSystemErrorText(error.Status())));
        return TError();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

void SetupCrashRecording(std::string processFingerprint)
{
    ProcessFingerprint.Store(processFingerprint);

    auto currentFileName = GetCurrentFileName(processFingerprint);
    auto lastFileName = GetPreviousFileName(processFingerprint);

    auto candidateCrashReport = ReadCrashReport(currentFileName);
    if (!candidateCrashReport.IsOK()) {
        NFs::Rename(currentFileName.c_str(), lastFileName.c_str());
    }

    auto mode = EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly | EOpenModeFlag::AWUser | EOpenModeFlag::ARUser;
    auto extraFileHandle = TFileHandle(currentFileName.c_str(), mode).Release();

    if (extraFileHandle == INVALID_FHANDLE) {
        WriteToStderr(Format("Can not setup crash recording to file %Qv; error: %v\n", currentFileName, LastSystemErrorText(LastSystemError())));
        return;
    }

    ExtraCrashOutputFileHandle.store(extraFileHandle);
    SetCrashHandlerWriter(&WriteToStderrAndExtraFd);
}

TError GetPreviousCrashError()
{
    std::string processFingerprint = ProcessFingerprint.Load();
    return ReadCrashReport(GetPreviousFileName(processFingerprint));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
