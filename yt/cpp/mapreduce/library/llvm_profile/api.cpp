#include "api.h"

#include <yt/cpp/mapreduce/library/blob/api.h>

#include <yt/cpp/mapreduce/interface/client.h>

#include <util/generic/guid.h>
#include <util/generic/singleton.h>
#include <util/random/entropy.h>
#include <util/random/fast.h>
#include <util/stream/file.h>
#include <util/stream/input.h>
#include <util/system/compiler.h>
#include <util/system/fs.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>
#include <util/stream/str.h>

extern "C" {
    void __llvm_profile_reset_counters();

    // replace with __llvm_profile_dump when available
    int __llvm_profile_write_file();
}

extern "C" {
    void Y_WEAK __llvm_profile_reset_counters() {
    }

    int Y_WEAK __llvm_profile_write_file() {
        return 0;
    }
}

namespace {
    struct TData {
        TMutex Mutex;
        TFastRng<ui64> PRNGForDecisionMaking;

        bool Started{false};
        NYT::TTransactionId TransactionID{};
        TString Proxy;
        TString Token;
        ::NYT::TYPath Table;

        TData()
            : PRNGForDecisionMaking{Seed()} {
        }
    };
}

void NYtLlvmProfile::StartProfiling(const TOptions& opts) {
    auto& d = *Singleton<TData>();
    with_lock (d.Mutex) {
        if (d.Started) {
            return;
        }

        if (d.PRNGForDecisionMaking.GenRandReal1() > opts.JobSamplingRate) {
            return;
        }

        Y_ENSURE(GetGuid(opts.TransactionID, d.TransactionID));
        d.Proxy = opts.Proxy;
        d.Token = opts.Token;
        d.Table = opts.Table;
        d.Started = true;

        __llvm_profile_reset_counters();
    }
}

void NYtLlvmProfile::CancelProfiling() noexcept {
    try {
        auto& d = *Singleton<TData>();
        with_lock (d.Mutex) {
            d.Started = false;
            __llvm_profile_reset_counters();
        }
    } catch (...) {
        // just in case;
    }
}

static void StopAndDumpProfile(TData& d) {
    TString profileToUpload;
    with_lock (d.Mutex) {
        if (!d.Started) {
            return;
        }

        if (__llvm_profile_write_file()) {
            return;
        }
        const auto profileFileName = TString{TStringBuf("default.profraw")};
        if (NFs::Exists(profileFileName)) {
            profileToUpload = TFileInput{profileFileName}.ReadAll();
            NFs::Remove(profileFileName);
        }
        d.Started = false;
    }

    if (profileToUpload) {
        const auto name = CreateGuidAsString() + ".profraw";
        auto tx = NYT::CreateClient(d.Proxy, NYT::TCreateClientOptions{}.Token(d.Token))
                    ->AttachTransaction(d.TransactionID);
        TStringInput in{profileToUpload};
        NYtBlob::Upload(in, name, d.Table, tx);
    }
}

void NYtLlvmProfile::StopAndDumpProfile() noexcept {
    try {
        auto& d = *Singleton<TData>();
        ::StopAndDumpProfile(d);
    } catch (...) {
    }
}
