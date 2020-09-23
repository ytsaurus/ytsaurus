#include <dlfcn.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>

#define MAX_FILENAME_LENGTH 1024
#define MAX_FILE_DESCRIPTORS_PER_PROCESS 1024

static const char* GPU_CORE_DUMP_PIPE_NAME = "yt_gpu_core_dump_pipe";

// NB(gritukan): There are tons on ways to do something with files in Linux
// and we don't handle most of them, so these values are almost random
// for arbitrary files. However, all the functions that I saw during
// reverse engineering of GPU core materializing pipeline are wrapped.
size_t FdToBytesWritten[MAX_FILE_DESCRIPTORS_PER_PROCESS];

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
            return FdToBytesWritten[fd];
        }
    }

    errno = wrapped_errno;
    return ret;
}

ssize_t write(int fd, const void* buf, size_t count)
{
    static ssize_t (*wrapped_write)(int fd, const void* buf, size_t count) = NULL;

    if (!wrapped_write) {
        wrapped_write = dlsym(RTLD_NEXT, "write");
    }
    ssize_t ret = wrapped_write(fd, buf, count);

    if (fd < MAX_FILE_DESCRIPTORS_PER_PROCESS) {
        FdToBytesWritten[fd] += ret;
    }

    return ret;
}

size_t fwrite(const void* ptr, size_t size, size_t count, FILE* stream)
{
    static size_t (*wrapped_fwrite)(const void* ptr, size_t size, size_t count, FILE* stream) = NULL;

    if (!wrapped_fwrite) {
        wrapped_fwrite = dlsym(RTLD_NEXT, "fwrite");
    }
    int fd = fileno(stream);
    size_t ret = wrapped_fwrite(ptr, size, count, stream);

    if (fd < MAX_FILE_DESCRIPTORS_PER_PROCESS) {
        FdToBytesWritten[fd] += ret * size;
    }

    return ret;
}
