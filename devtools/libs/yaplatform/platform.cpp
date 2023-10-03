#include "platform.h"

#include <util/string/join.h>
#include <util/string/split.h>
#include <util/generic/maybe.h>
#include <util/system/error.h>

#ifndef _win_
    #include <sys/utsname.h>
#endif

namespace NYa {
    TCanonizedPlatform::TCanonizedPlatform(const TString& canonizedString) {
        TMaybe<TStringBuf> arch{};
        try {
            Split(to_lower(canonizedString), PLATFORM_SEP, Os_, arch);
        } catch (const yexception& e) {
            throw yexception() << "Wrong platform string: " << canonizedString;
        }
        Arch_ = arch.GetOrElse(DEFAULT_ARCH);
        Check();
    }

    TCanonizedPlatform::TCanonizedPlatform(const TString& os, const TString& arch)
        : Os_{to_lower(os)}
        , Arch_{Os_ == ANY_PLATFORM ? DEFAULT_ARCH : to_lower(arch)}
    {
        Check();
    }

    TString TCanonizedPlatform::AsString() const {
        if (Arch_ == DEFAULT_ARCH || Os_ == ANY_PLATFORM) {
            return Os_;
        } else {
            return Join(PLATFORM_SEP, Os_, Arch_);
        }
    }

    void TCanonizedPlatform::Check() const {
        Y_ENSURE(Os_, "OS should not be empty");
        Y_ENSURE(Arch_, "Architecture should not be empty");
        Y_ENSURE(!Os_.Contains(PLATFORM_SEP));
        Y_ENSURE(!Arch_.Contains(PLATFORM_SEP));
    }

    const TPlatformReplacements& DefaultPlatformReplacements() {
        const static TPlatformReplacements PLATFORM_REPLACEMENTS = {
            {{"darwin", "arm64"}, {{"darwin", "x86_64"}}}
        };
        return PLATFORM_REPLACEMENTS;
    };

#ifndef _win32_
    static utsname Uname() {
        utsname result{};
        if (uname(&result)) {
            throw yexception() << "uname() failed: " << LastSystemErrorText(errno);
        }
        return result;
    }
#endif

#if defined (_win_)
    TString CurrentOs() {
       return "WIN";
    }

    TString CurrentArchitecture() {
    #if defined(_x86_64_)
        return "X86_64";
    #else
        #error Unsupported Windows architecture
    #endif
    }

    TCanonizedPlatform MyPlatform() {
    #if defined(_x86_64_)
        return TCanonizedPlatform{"win32"};
    #else
        #error Unsupported Windows architecture
    #endif
    }
#else
    TString CurrentOs() {
        auto s_uname = Uname();
        return to_upper(TString(s_uname.sysname));
    }

    TString CurrentArchitecture() {
        auto s_uname = Uname();
        TString machine{s_uname.machine};
        machine.to_upper();
        if (machine == "AMD64") {
            return "X86_64";
        }
        return machine;
    }

    TCanonizedPlatform MyPlatform() {
        auto s_uname = Uname();
        TString machine{s_uname.machine};
        machine.to_lower();
        return TCanonizedPlatform{s_uname.sysname, machine};
    }
#endif

    TLegacyPlatform CurrentPlatform() {
        return {CurrentOs(), CurrentArchitecture()};
    }

    bool IsDarwinArm64() {
#if defined(_darwin_)
        auto s_uname = Uname();
        TStringBuf machine{s_uname.machine};
        TStringBuf version{s_uname.version};
        // Native or Rosetta
        return machine == "arm64" || version.contains("/RELEASE_ARM64");
#else
        return false;
#endif
    }

    static TString ComputeArch(const TString& platform) {
        for (const TStringBuf arch : {"aarch64", "ppc64le", "arm64"}) {
            if (platform.Contains(arch)) {
                return TString{arch};
            }
        }
        return TString{TCanonizedPlatform::DEFAULT_ARCH};
    }

    TCanonizedPlatform CanonizePlatform(TString platform) {
        platform.to_lower();
        if (platform == "any") {
            return TCanonizedPlatform{platform};
        } else if (platform.StartsWith("linux")) {
            return  {"linux", ComputeArch(platform)};
        } else if (platform.StartsWith("darwin")) {
            return {"darwin", ComputeArch(platform)};
        } else if (platform.StartsWith("win")) {
            return TCanonizedPlatform{"win32"};
        }
        throw yexception() << "Unsupported platform: '" << platform << "'";
    }

    TString MatchPlatform(TCanonizedPlatform expect, const TVector<TString>& platforms, const TPlatformReplacements* platformReplacements) {
        THashMap<TCanonizedPlatform, TString> canonizedPlatformMap;
        for (const TString& platform : platforms) {
            TCanonizedPlatform canonized = CanonizePlatform(platform);
            if (canonizedPlatformMap.contains(canonized)) {
                throw yexception() << "Platform '" << platform << "' and '" << canonizedPlatformMap[canonized]
                    << "' have the same canonized form '" << canonized.AsString() << "'";
            }
            canonizedPlatformMap.emplace(canonized, platform);
        }

        if (const TString* platform = canonizedPlatformMap.FindPtr(expect)) {
            return *platform;
        }

        if (!platformReplacements) {
            platformReplacements = &DefaultPlatformReplacements();
        }

        if (const auto replacements = platformReplacements->FindPtr(expect)) {
            for (const TCanonizedPlatform& replacement : *replacements) {
                if (const TString* platform = canonizedPlatformMap.FindPtr(replacement)) {
                    return *platform;
                }
            }
        }
        if (const TString* platform = FindPtr(platforms, TCanonizedPlatform::ANY_PLATFORM)) {
            return *platform;
        }
        return {};
    }
}
