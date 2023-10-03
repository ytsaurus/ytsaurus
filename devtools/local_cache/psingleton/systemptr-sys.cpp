#include "systemptr.h"
#include <util/stream/file.h>
#include <util/string/split.h>
#include <util/system/file.h>

#if defined(_darwin_)
#include <sys/sysctl.h>
#endif

namespace NSystemWideName {
    time_t GetProcessCtime(TProcessId pid) noexcept {
        timeval tv;
        Zero(tv);
#if defined(__linux__)
        // The time the process started after system boot. In kernels before Linux 2.6, this value
        // was expressed in jiffies.  Since Linux 2.6, the value is expressed  in  clock  ticks
        // (divide by sysconf(_SC_CLK_TCK)).
        //
        // The format for this field was %lu before Linux 2.6.

        TString path("/" + JoinFsPaths("proc", ToString(pid), "stat"));
        try {
            TFile stat(path, OpenExisting | RdOnly);
            auto content = TUnbufferedFileInput(stat).ReadAll();
            auto afterCmd = StringSplitter(content).Split(')').Limit(2).Take(2).ToList<TString>().back();
            auto info = StringSplitter(afterCmd).Split(' ').SkipEmpty().Limit(21).Take(20).ToList<TString>();
            return info[0][0] == 'Z' || info[0][0] == 'x' || info[0][0] == 'X' ? 0 : FromString<i64>(info.back());
        } catch (const yexception&) {
            return 0;
        }
#elif defined(_win_)
        // https://a.yandex-team.ru/arc/trunk/arcadia/strm/cv/ffmpeg_adcv/ffmpeg_adcv.c?rev=5190994#L4495
        // https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/psutil/src/psutil/_psutil_windows.c
        DWORD dwDesiredAccess = PROCESS_QUERY_INFORMATION | PROCESS_VM_READ;
        if (pid == 0) {
            return 0;
        }
        HANDLE process = OpenProcess(dwDesiredAccess, FALSE, pid);
        if (process == nullptr) {
            return 0;
        }

        FILETIME c, e, k, u;
        if (GetProcessTimes(process, &c, &e, &k, &u)) {
            CloseHandle(process);
            FileTimeToTimeval(&c, &tv);
        } else {
            CloseHandle(process);
            return 0;
        }
#else
        // https://a.yandex-team.ru/arc/trunk/arcadia/contrib/python/psutil/src/psutil/_psutil_osx.c?rev=4235773#L218
        struct kinfo_proc kp;
        Zero(kp);
        int mib[4];
        size_t len = sizeof(kp);
        mib[0] = CTL_KERN;
        mib[1] = KERN_PROC;
        mib[2] = KERN_PROC_PID;
        mib[3] = (pid_t)pid;

        if (sysctl(mib, 4, &kp, &len, NULL, 0) == -1 || len == 0) {
            return 0;
        }
        if (kp.kp_proc.p_stat != SZOMB && (kp.kp_proc.p_flag & P_WEXIT) == 0) {
            tv = kp.kp_proc.p_starttime;
        }
#endif
        return tv.tv_sec * 1000 + tv.tv_usec / 1000;
    }
}
