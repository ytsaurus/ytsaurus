#include "pstat.h"

#include <library/cpp/yt/string/format.h>

#include <util/folder/iterator.h>

#include <util/string/vector.h>

#include <util/stream/file.h>

#include <util/system/file.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> ReadProcessStat(TProcessId processId)
{
    try {
        auto filename = Format("/proc/%v/stat", processId);
        auto handle = ::TFileHandle(filename, OpenExisting | RdOnly | Seq);
        if (!handle.IsOpen()) {
            return {};
        }
        TIFStream pstat(::TFile(handle.Release(), filename));
        return SplitString(pstat.ReadLine(), " ");
    } catch (const TFileError& err) {
        return {};
    }
}

TString GetProcessCommand(TProcessId processId)
{
    auto pstat = ReadProcessStat(processId);
    if (pstat.empty()) {
        return {};
    }
    auto command = pstat[ToUnderlying(EProcessStatField::Command)];
    return TString(command.data(), 1, command.size() - 2);
}

std::vector<TProcessId> FindProcessIds(TString command)
{
    std::vector<TProcessId> result;

    TDirIterator dir("/proc", TDirIterator::TOptions().SetMaxLevel(1));

    for (const auto& dentry : dir) {

        if (dentry.fts_info != FTS_D) {
            continue;
        }
        auto processId = TProcessId(dentry.fts_name);
        if (GetProcessCommand(processId) == command) {
            result.push_back(processId);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
