#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main()
{
    if (strcmp(getenv("CUDA_ENABLE_COREDUMP_ON_EXCEPTION"), "1") != 0) {
        fprintf(stderr, "CUDA_ENABLE_COREDUMP_ON_EXCEPTION is not properly set\n");
        return 1;
    }

    char* cudaCoreDumpFile = getenv("CUDA_COREDUMP_FILE");
    FILE* file = fopen(cudaCoreDumpFile, "a");
    if (!file) {
        fprintf(stderr, "fopen\n");
        return 2;
    }

    const int iterations = 10000;
    for (int i = 0; i < iterations; ++i) {
        const int bufSize = 100;
        char buf[bufSize];
        memset(buf, 'a', sizeof(buf));
        int bufPtr = 0;
        while (bufPtr < bufSize) {
            int written = fwrite(buf, sizeof(char), bufSize - bufPtr, file);
            if (written < 0) {
                fprintf(stderr, "fwrite\n");
                return 4;
            }
            bufPtr += written;
        }
        if (ftell(file) < 0) {
            fprintf(stderr, "ftell\n");
            return 5;
        }
    }

    fclose(file);

    return 0;
}
