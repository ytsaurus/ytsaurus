#pragma once

#include "wavm_private_imports.h"

#include <util/system/types.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

WAVM_DECLARE_INTRINSIC_MODULE(empty);
WAVM_DECLARE_INTRINSIC_MODULE(standard);

////////////////////////////////////////////////////////////////////////////////

#define FOREACH_WEB_ASSEMBLY_SYSCALL(macro) \
    macro(socket, i32, i32, i32, i32); \
    macro(fd_close, i32, i32); \
    macro(send, i64, i32, i64, i64, i32); \
    macro(recv, i64, i32, i64, i64, i32); \
    macro(connect, i32, i32, i64, i32); \
    macro(bind, i32, i32, i64, i32); \
    macro(setsockopt, i32, i32, i32, i32, i64, i32); \
    macro(sendto, i64, i32, i64, i64, i32, i64, i32); \
    macro(recvfrom, i64, i32, i64, i64, i32, i64, i64); \
    macro(__syscall_recvmmsg, i32, i32, i64, i64, i32, i64); \
    macro(sendmsg, i64, i32, i64, i32); \
    macro(getnameinfo, i32, i64, i32, i64, i32, i64, i32, i32); \
    macro(environ_sizes_get, i32, i64, i64); \
    macro(environ_get, i32, i64, i64); \
    macro(__syscall_umask, i32, i32); \
    macro(__syscall_mkdirat, i32, i32, i64, i32); \
    macro(__syscall_chmod, i32, i64, i32); \
    macro(__syscall_fchmodat, i32, i32, i64, i32, i64); \
    macro(__syscall_newfstatat, i32, i32, i64, i64, i32); \
    macro(__syscall_utimensat, i32, i32, i64, i64, i32); \
    macro(__syscall_mknodat, i32, i32, i64, i32, i32); \
    macro(__syscall_fchmod, i32, i32, i32); \
    macro(__syscall_statfs64, i32, i64, i64, i64); \
    macro(__syscall_fstatfs64, i32, i32, i64, i64); \
    macro(__syscall_stat64, i32, i64, i64); \
    macro(__syscall_lstat64, i32, i64, i64); \
    macro(fd_sync, i32, i32); \
    macro(__syscall_fchownat, i32, i32, i64, i32, i32, i32); \
    macro(__syscall_setsid, i32); \
    macro(__syscall_renameat, i32, i32, i64, i32, i64); \
    macro(__syscall_getsid, i32, i32); \
    macro(__syscall_symlinkat, i32, i64, i32, i64); \
    macro(__syscall_getpgid, i32, i32); \
    macro(__syscall_pause, i32); \
    macro(__syscall_dup3, i32, i32, i32, i32); \
    macro(__syscall_pipe, i32, i64); \
    macro(__syscall_getgroups32, i32, i32, i64); \
    macro(__syscall_setpgid, i32, i32, i32); \
    macro(fd_pwrite, i32, i32, i64, i64, i64, i64); \
    macro(__syscall_geteuid32, i32); \
    macro(__syscall_readlinkat, i32, i32, i64, i64, i64); \
    macro(__syscall_getuid32, i32); \
    macro(fd_read, i32, i32, i64, i64, i64); \
    macro(__syscall_getgid32, i32); \
    macro(__syscall_rmdir, i32, i64); \
    macro(__syscall_acct, i32, i64); \
    macro(__syscall_fdatasync, i32, i32); \
    macro(fd_write, i32, i32, i64, i64, i64); \
    macro(__syscall_faccessat, i32, i32, i64, i32, i32); \
    macro(fd_fdstat_get, i32, i32, i64); \
    macro(__syscall_getcwd, i32, i64, i64); \
    macro(__syscall_chdir, i32, i64); \
    macro(__syscall_getegid32, i32); \
    macro(__syscall_truncate64, i32, i64, i64); \
    macro(__syscall_unlinkat, i32, i32, i64, i32); \
    macro(__syscall_pipe2, i32, i64, i32); \
    macro(__syscall_link, i32, i64, i64); \
    macro(__syscall_dup, i32, i32); \
    macro(__syscall_sync, i32); \
    macro(fd_pread, i32, i32, i64, i64, i64, i64); \
    macro(__syscall_linkat, i32, i32, i64, i32, i64, i32); \
    macro(fd_seek, i32, i32, i64, i32, i64); \
    macro(__syscall_ftruncate64, i32, i32, i64); \
    macro(__syscall_symlink, i32, i64, i64); \
    macro(__syscall_getppid, i32); \
    macro(__syscall_fchown32, i32, i32, i32, i32); \
    macro(__syscall_fchdir, i32, i32); \
    macro(__subtf3, void, i64, i64, i64, i64, i64); \
    macro(__divtf3, void, i64, i64, i64, i64, i64); \
    macro(__syscall_fadvise64, i32, i32, i64, i64, i32); \
    macro(__syscall_fallocate, i32, i32, i32, i64, i64); \
    macro(__syscall_getdents64, i32, i32, i64, i64); \
    macro(__eqtf2, i32, i64, i64, i64, i64); \
    macro(__multf3, void, i64, i64, i64, i64, i64); \
    macro(__letf2, i32, i64, i64, i64, i64); \
    macro(__netf2, i32, i64, i64, i64, i64); \
    macro(__syscall_mincore, i32, i64, i64, i64); \
    macro(__syscall_mremap, i32, i64, i64, i64, i32, i64); \
    macro(__syscall_mprotect, i32, i64, i64, i32); \
    macro(__syscall_munlockall, i32); \
    macro(__syscall_munlock, i32, i64, i64); \
    macro(__syscall_madvise, i32, i64, i64, i32); \
    macro(__syscall_mlock, i32, i64, i64); \
    macro(__syscall_mlockall, i32, i32); \
    macro(__syscall__newselect, i32, i32, i64, i64, i64, i64); \
    macro(__syscall_poll, i32, i64, i32, i32); \
    macro(__syscall_pselect6, i32, i32, i64, i64, i64, i64, i64); \
    macro(__mulsc3, void, i64, float, float, float, float); \
    macro(__lttf2, i32, i64, i64, i64, i64); \
    macro(__trunctfdf2, double, i64, i64); \
    macro(__extenddftf2, void, i64, double); \
    macro(__muldc3, void, i64, double, double, double, double); \
    macro(__addtf3, void, i64, i64, i64, i64, i64); \
    macro(__unordtf2, i32, i64, i64, i64, i64); \
    macro(__multc3, void, i64, i64, i64, i64, i64, i64, i64, i64, i64); \
    macro(__trunctfsf2, float, i64, i64); \
    macro(__extendsftf2, void, i64, float); \
    macro(__getf2, i32, i64, i64, i64, i64); \
    macro(__fixtfdi, i64, i64, i64); \
    macro(__floatditf, void, i64, i64); \
    macro(__multi3, void, i64, i64, i64, i64, i64); \
    macro(getpwnam_r, i32, i64, i64, i64, i64, i64); \
    macro(getpwuid_r, i32, i32, i64, i64, i64, i64); \
    macro(execve, i32, i64, i64, i64); \
    macro(__syscall_wait4, i32, i32, i64, i32, i32); \
    macro(__floatsitf, void, i64, i32); \
    macro(__gttf2, i32, i64, i64, i64, i64); \
    macro(__fixtfsi, i32, i64, i64); \
    macro(emscripten_stack_get_base, i64); \
    macro(emscripten_stack_get_current, i64); \
    macro(__syscall_prlimit64, i32, i32, i32, i64, i64); \
    macro(__syscall_ugetrlimit, i32, i32, i64); \
    macro(__syscall_setdomainname, i32, i64, i64); \
    macro(__syscall_setrlimit, i32, i32, i64); \
    macro(fork, i32); \
    macro(__syscall_getresuid32, i32, i64, i64, i64); \
    macro(__syscall_getpriority, i32, i32, i32); \
    macro(__syscall_setpriority, i32, i32, i32, i32); \
    macro(__syscall_uname, i32, i64); \
    macro(__syscall_getresgid32, i32, i64, i64, i64); \
    macro(__syscall_getrusage, i32, i32, i64); \
    macro(_emscripten_get_progname, void, i64, i32); \
    macro(__floatunsitf, void, i64, i32); \
    macro(proc_exit, void, i32); \
    macro(_setitimer_js, i32, i32, double); \
    macro(_dlopen_js, i64, i64); \
    macro(_emscripten_dlopen_js, void, i64, i64, i64, i64); \
    macro(_dlsym_js, i64, i64, i64, i64); \
    macro(_dlinit, void, i64); \
    macro(emscripten_stack_get_end, i64); \
    macro(_msync_js, i32, i64, i64, i32, i32, i32, i64); \
    macro(_tzset_js, void, i64, i64, i64); \
    macro(_localtime_js, void, i64, i64); \
    macro(_gmtime_js, void, i64, i64); \
    macro(emscripten_date_now, double); \
    macro(_emscripten_get_now_is_monotonic, i32); \
    macro(emscripten_get_now_res, double); \
    macro(args_sizes_get, i32, i64, i64); \
    macro(args_get, i32, i64, i64); \
    macro(__main_argc_argv, i32, i32, i64); \
    macro(clock_res_get, i32, i32, i64); \
    macro(clock_time_get, i32, i32, i64, i64); \
    macro(__divti3, void, i64, i64, i64, i64, i64); \
    macro(__lshrti3, void, i64, i64, i64, i32); \
    macro(_dlsym_catchup_js, i64, i64, i32); \
    macro(emscripten_promise_resolve, void, i64, i32, i64); \
    macro(emscripten_proxy_callback, i32, i64, i64, i64, i64, i64, i64); \
    macro(emscripten_promise_destroy, void, i64); \
    macro(_emscripten_dlsync_threads, void, void); \
    macro(emscripten_promise_create, i64); \
    macro(_emscripten_dlsync_threads_async, void, i64, i64, i64); \
    macro(getsockname, i32, i32, i64, i64); \
    macro(_ZNK4NTls4TKey3GetEv, i64, i64); \
    macro(_ZNK4NTls4TKey3SetEPv, void, i64, i64); \
    macro(_ZN4NTls4TKeyC1EPFvPvE, i64, i64, i64); \
    macro(_Z6AtExitPFvPvES_m, void, i64, i64, i64); \
    macro(_ZN4NTls4TKeyD1Ev, i64, i64); \
    macro(_Z6GetPIDv, i32); \
    macro(_Z12MicroSecondsv, i64); \
    macro(_Z3OutI15TSourceLocationEvR13IOutputStreamN11TTypeTraitsIT_E10TFuncParamE, void, i64, i64); \
    macro(_ZN7TThread15CurrentThreadIdEv, i64); \
    macro(_ZN9TSpinWaitC1Ev, i64, i64); \
    macro(_ZN9TSpinWait5SleepEv, void, i64); \
    macro(_ZNK10TBackTrace13PrintToStringEv, void, i64, i64); \
    macro(_Z19LastSystemErrorTexti, i64, i32); \
    macro(_ZN10TBackTraceC1Ev, i64, i64); \
    macro(_ZN10TBackTrace7CaptureEv, void, i64); \
    macro(_ZN10TMemoryMapC1ERK12TBasicStringIcNSt4__y111char_traitsIcEEE6TFlagsIN16TMemoryMapCommon13EOpenModeFlagEE, i64, i64, i64, i32); \
    macro(_ZNK10TMemoryMap6LengthEv, i64, i64); \
    macro(_ZN10TMemoryMapD1Ev, i64, i64); \
    macro(_ZN16TMemoryMapCommon15UnknownFileNameEv, i64); \
    macro(_ZN10TMemoryMapC1ERK5TFile6TFlagsIN16TMemoryMapCommon13EOpenModeFlagEERK12TBasicStringIcNSt4__y111char_traitsIcEEE, i64, i64, i64, i32, i64); \
    macro(_ZN5TFileC1ERK12TBasicStringIcNSt4__y111char_traitsIcEEE6TFlagsI13EOpenModeFlagE, i64, i64, i64, i32); \
    macro(_ZNK5TFile9GetLengthEv, i64, i64); \
    macro(_ZNK5TFile5PloadEPvmx, void, i64, i64, i64, i64); \
    macro(_ZN5TFileD1Ev, i64, i64); \
    macro(_ZN8TFileMapC1ERK10TMemoryMap, i64, i64, i64); \
    macro(_ZNK10TMemoryMap6IsOpenEv, i32, i64); \
    macro(_ZN8TFileMap3MapExm, void, i64, i64, i64, i64); \
    macro(_Z10LockMemoryPKvm, void, i64, i64); \
    macro(_ZN8TFileMapD1Ev, i64, i64); \
    macro(_Z12UnlockMemoryPKvm, void, i64, i64); \
    macro(_ZN17TMappedAllocationC1EmbPv, i64, i64, i64, i32, i64); \
    macro(_ZN17TMappedAllocation7DeallocEv, void, i64); \
    macro(_ZN17TMappedAllocation4swapERS_, void, i64, i64); \
    macro(_ZN11NSystemInfo18CachedNumberOfCpusEv, i64); \
    macro(_ZN11NSystemInfo15TotalMemorySizeEv, i64); \
    macro(_Z8HostNamev, i64); \
    macro(_Z11GetExecPathv, i64); \
    macro(_ZN11NSystemInfo11LoadAverageEPdm, i64, i64, i64); \
    macro(_ZN8NMemInfo10GetMemInfoEi, void, i64, i32); \
    macro(_ZN7TRusage4FillEv, void, i64); \
    macro(_Z8CpuBrandPj, i64, i64); \
    macro(_ZN3NFs23CurrentWorkingDirectoryEv, void, i64); \
    macro(_Z15LastSystemErrorv, i32); \
    macro(_ZN21TDirectIOBufferedFile5PreadEPvmy, i64, i64, i64, i64, i64); \
    macro(_ZN21TDirectIOBufferedFile5WriteEPKvm, void, i64, i64, i64); \
    macro(_ZN21TDirectIOBufferedFile9FlushDataEv, void, i64); \
    macro(_ZN21TDirectIOBufferedFileC1ERK12TBasicStringIcNSt4__y111char_traitsIcEEE6TFlagsI13EOpenModeFlagEm, i64, i64, i64, i32, i64); \
    macro(_ZN21TDirectIOBufferedFile6FinishEv, void, i64); \
    macro(_ZN5TFileC1EPKc6TFlagsI13EOpenModeFlagE, i64, i64, i64, i32); \
    macro(_ZNK5TFile6IsOpenEv, i32, i64); \
    macro(_ZNK5TFile7GetNameEv, i64, i64); \
    macro(_ZN5TFileC1ERKNSt4__y14__fs10filesystem4pathE6TFlagsI13EOpenModeFlagE, i64, i64, i64, i32); \
    macro(_ZN5TFile10ReadOrFailEPvm, i64, i64, i64, i64); \
    macro(_ZNK5TFile11GetPositionEv, i64, i64); \
    macro(_ZN5TFile4SeekEx7SeekDir, i64, i64, i64, i32); \
    macro(_ZN5TFile5WriteEPKvm, void, i64, i64, i64); \
    macro(_ZN5TFile5FlushEv, void, i64); \
    macro(popen, i64, i64, i64); \
    macro(pclose, i32, i64); \
    macro(_ZN11TPipeHandle5CloseEv, i32, i64); \
    macro(_ZNK11TPipeHandle4ReadEPvm, i64, i64, i64, i64); \
    macro(_ZNK11TPipeHandle5WriteEPKvm, i64, i64, i64, i64); \
    macro(_ZN21TDirectIOBufferedFileD1Ev, i64, i64); \
    macro(_Z20InitNetworkSubSystemv, void); \
    macro(_Z9NanoSleepy, void, i64); \
    macro(_ZN8NHPTimer18GetCyclesPerSecondEv, i64); \
    macro(_Z14PrintBackTracev, void); \
    macro(_Z8TypeNameRKSt9type_info, void, i64, i64); \
    macro(getpwnam, i64, i64); \
    macro(getpwuid, i64, i32); \
    macro(_Z20ClearLastSystemErrorv, void); \
    macro(_Unwind_Backtrace, i32, i64, i64); \
    macro(getdtablesize, i32); \
    macro(flock, i32, i32, i32); \
    macro(_ZN15TNetworkAddressC1ERK12TBasicStringIcNSt4__y111char_traitsIcEEEt, i64, i64, i64, i32); \
    macro(_ZN15TNetworkAddressD1Ev, i64, i64); \
    macro(getloadavg, i32, i64, i32); \
    macro(getaddrinfo, i32, i64, i64, i64, i64); \
    macro(freeaddrinfo, void, i64); \
    macro(_ZN20TCurrentThreadLimitsC1Ev, i64, i64); \
    macro(getgrouplist, i32, i64, i32, i64, i64); \
    macro(setgroups, i32, i64, i64); \
    macro(_ZN7TThreadC1EPFPvS0_ES0_, i64, i64, i64, i64); \
    macro(_ZN7TThread5StartEv, void, i64); \
    macro(_Z11SetNonBlockib, void, i32, i32); \
    macro(_Z5PollDP6pollfdmRK8TInstant, i64, i64, i64, i64); \
    macro(_ZN7TThread4JoinEv, i64, i64); \
    macro(shmget, i32, i32, i64, i32); \
    macro(shmat, i64, i32, i64, i32); \
    macro(shmctl, i32, i32, i32, i64); \
    macro(shmdt, i32, i64); \
    macro(_ZN7TThreadD1Ev, i64, i64); \
    macro(_ZN7TThread6DetachEv, void, i64); \
    macro(_ZN7TThread20SetCurrentThreadNameEPKc, void, i64);

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_WEB_ASSEMBLY_SYSCALL(name, result, ...) \
    result name(__VA_ARGS__);

    DECLARE_WEB_ASSEMBLY_SYSCALL(emscripten_notify_memory_growth, void, i64);
    FOREACH_WEB_ASSEMBLY_SYSCALL(DECLARE_WEB_ASSEMBLY_SYSCALL);

#undef DECLARE_WEB_ASSEMBLY_SYSCALL_STUB

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
