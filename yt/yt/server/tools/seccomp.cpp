#include "seccomp.h"

#include <yt/yt/core/misc/common.h>
#include <yt/yt/core/misc/error.h>

#ifdef __linux__

#include <errno.h>
#include <stddef.h>
#include <string.h>
#include <sys/prctl.h>
#include <sys/utsname.h>
#include <linux/seccomp.h>
#include <linux/audit.h>
#include <linux/filter.h>
#include <linux/unistd.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

static int CompareKernelVersion(const char *version)
{
    struct utsname uts_buf;
    if (uname(&uts_buf) != 0) {
        return -1;
    }

    return strverscmp(uts_buf.release, version);
}

static void SeccompInit(int arch, int error)
{
    #define BPF_LOG(syscall) \
        BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, __NR_##syscall, 0, 1), \
        BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_LOG)

    struct sock_filter filter[] = {
        /* validate arch */
        BPF_STMT(BPF_LD + BPF_W + BPF_ABS, (offsetof(struct seccomp_data, arch))),
        BPF_JUMP(BPF_JMP + BPF_JEQ + BPF_K, static_cast<ui32>(arch), 1, 0),
        BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ERRNO | (error & SECCOMP_RET_DATA)),

        /* load syscall */
        BPF_STMT(BPF_LD + BPF_W + BPF_ABS, (offsetof(struct seccomp_data, nr))),

        /* list of logged syscalls */
        BPF_LOG(execve),
        BPF_LOG(execveat),
        BPF_LOG(ptrace),
        BPF_LOG(connect),

        /* that's fine, allow any unknown syscall to execute  */
        BPF_STMT(BPF_RET + BPF_K, SECCOMP_RET_ALLOW),
    };

    struct sock_fprog prog = {
        .len = (unsigned short)(sizeof(filter) / sizeof(filter[0])),
        .filter = filter,
    };

    if (prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) {
        THROW_ERROR_EXCEPTION("Failed to limit child process privileges")
            << TError::FromSystem();
    }

    if (prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog)) {
        THROW_ERROR_EXCEPTION("Failed to install seccomp ebpf filter")
            << TError::FromSystem();
    }
}

void SetupSeccomp()
{
    if (CompareKernelVersion("4.19") < 0) {
        // Don't setup seccomp for old kernels: https://st.yandex-team.ru/SKYDEV-2137.
        return;
    }

    SeccompInit(AUDIT_ARCH_X86_64, EPERM);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools

#else

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

void SetupSeccomp()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools

#endif
