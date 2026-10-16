#pragma once

#include <yql/essentials/utils/yql_panic.h>

#include <util/folder/pathsplit.h>
#include <util/generic/hash.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NYql::NDq::NWorker::NDetail {

    inline void ConfigureChildLdLibraryPath(
        THashMap<TString, TString>* environment,
        bool useLocalLdLibraryPath,
        bool enablePorto,
        TStringBuf jobSandboxPath)
    {
        if (!useLocalLdLibraryPath) {
            return;
        }

        if (enablePorto) {
            (*environment)["LD_LIBRARY_PATH"] = ".";
            return;
        }

        YQL_ENSURE(
            TPathSplitTraitsLocal::IsAbsolutePath(jobSandboxPath),
            "YT job sandbox path for LD_LIBRARY_PATH must be absolute, got: " << jobSandboxPath);
        (*environment)["LD_LIBRARY_PATH"] = TString::Join(jobSandboxPath, ":.");
    }

} // namespace NYql::NDq::NWorker::NDetail
