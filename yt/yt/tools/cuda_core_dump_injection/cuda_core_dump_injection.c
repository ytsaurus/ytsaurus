#include <dlfcn.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

#define MAX_FILENAME_LENGTH 1024

static const char* GPU_CORE_DUMP_PIPE_NAME = "yt_gpu_core_dump_pipe";

long int ftell(FILE* file)
{
    static long int (*wrapped_ftell)(FILE* file) = NULL;

    if (!wrapped_ftell) {
        wrapped_ftell = dlsym(RTLD_NEXT, "ftell");
    }
    int ret = wrapped_ftell(file);
    int wrapped_errno = errno;

    if (ret < 0) {
        char linkName[MAX_FILENAME_LENGTH];
        int fd = fileno(file);
        sprintf(linkName, "/proc/self/fd/%d", fd);

        char fileName[MAX_FILENAME_LENGTH];
        ssize_t fileNameLength = readlink(linkName, fileName, MAX_FILENAME_LENGTH);
        if (fileNameLength < 0) {
            errno = wrapped_errno;
            return -1;
        }

        if ((size_t)fileNameLength >= strlen(GPU_CORE_DUMP_PIPE_NAME) &&
            strncmp(fileName + fileNameLength - strlen(GPU_CORE_DUMP_PIPE_NAME), GPU_CORE_DUMP_PIPE_NAME, strlen(GPU_CORE_DUMP_PIPE_NAME)) == 0)
        {
            errno = wrapped_errno;
            return 0;
        }
    }

    errno = wrapped_errno;
    return ret;
}
