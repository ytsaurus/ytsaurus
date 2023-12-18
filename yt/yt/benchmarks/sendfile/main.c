#include <fcntl.h>

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sendfile.h>

#include <time.h>
#include <unistd.h>

static const uint64_t TEST_FILE_SIZE = 1024 * 1024 * 1024;
static const uint64_t BUF_SIZE = 4 * 1024;

static int write_test_file(const char *file_path) {
    if (access(file_path, F_OK) == 0) {
        return 0;
    }

    FILE *fp = fopen(file_path, "w");

    if (fp == NULL) {
        printf("Error opening the file %s", file_path);
        return -1;
    }

    for (uint64_t i = 0; i < TEST_FILE_SIZE / BUF_SIZE; i++) {
        char byte_array[BUF_SIZE];

        for (uint64_t j = 0; j < BUF_SIZE; j++) {
            byte_array[j] = (char)(32 + rand() % 57);
        }

        byte_array[BUF_SIZE - 1] = 0;

        fputs(&byte_array[0], fp);
    }

    return fclose(fp);
}

static double ticks_to_ms(uint64_t ticks) {
    return (ticks * 1000.) / CLOCKS_PER_SEC;
}

static int read_write_call(const char *from_file, const char *to_file) {
    struct stat stat_buf;
    char buf[BUF_SIZE];
    int from_fd, to_fd, wrote_bytes, i;
    uint64_t read_start, write_start, read_delta, write_delta;

    uint64_t read_max, read_sum, read_min;
    uint64_t write_max, write_sum, write_min;

    from_fd = open(from_file, O_RDONLY);
    fstat(from_fd, &stat_buf);
    to_fd = open(to_file, O_WRONLY | O_CREAT, stat_buf.st_mode);

    wrote_bytes = 1;
    i = 0;

    write_max = 0;
    write_min = UINT64_MAX;
    write_sum = 0;

    read_max = 0;
    read_min = UINT64_MAX;
    read_sum = 0;

    do {
        read_start = clock();
        wrote_bytes = read(from_fd, &buf, sizeof(buf));
        read_delta = clock() - read_start;

        read_sum += read_delta;

        if (read_delta > read_max) {
            read_max = read_delta;
        }

        if (read_delta < read_min) {
            read_min = read_delta;
        }

        write_start = clock();
        write(to_fd, &buf, wrote_bytes);
        write_delta = clock() - write_start;

        write_sum += write_delta;

        if (write_delta > write_max) {
            write_max = write_delta;
        }

        if (write_delta < write_min) {
            write_min = write_delta;
        }

        i++;
    } while(wrote_bytes > 0);

    printf("Read/Writes  %10s %10s %10s %10s  %10s\n", "Max", "Min", "Avg", "Count", "Sum");
    printf("Read         %10lf %10lf %10lf %10d  %10lf\n", ticks_to_ms(read_max), ticks_to_ms(read_min), ticks_to_ms(read_sum / i), i, ticks_to_ms(read_sum));
    printf("Write        %10lf %10lf %10lf %10d  %10lf\n", ticks_to_ms(write_max), ticks_to_ms(write_min), ticks_to_ms(write_sum / i), i, ticks_to_ms(write_sum));

    return 0;
}

static int sendfile_call(const char *from_file, const char *to_file) {
    struct stat stat_buf;
    int from_fd, to_fd, wrote_bytes, i;
    uint64_t write_start, write_delta;
    uint64_t write_max, write_sum, write_min;

    from_fd = open(from_file, O_RDONLY);

    fstat(from_fd, &stat_buf);

    to_fd = open(to_file, O_WRONLY | O_CREAT, stat_buf.st_mode);

    write_max = 0;
    write_min = UINT64_MAX;
    write_sum = 0;

    wrote_bytes = 1;
    i = 0;

    while (wrote_bytes > 0) {
        write_start = clock();
        wrote_bytes = sendfile(to_fd, from_fd, 0, BUF_SIZE);
        write_delta = clock() - write_start;

        write_sum += write_delta;

        if (write_delta > write_max) {
            write_max = write_delta;
        }

        if (write_delta < write_min) {
            write_min = write_delta;
        }

        i++;
    }

    printf("Sendfile     %10lf %10lf %10lf %10d  %10lf\n", ticks_to_ms(write_max), ticks_to_ms(write_min), ticks_to_ms(write_sum / i), i, ticks_to_ms(write_sum));

    return 0;
}

int main(int argc, char **argv) {
    const char *from_file, *to_file;

    if (argc == 3) {
        from_file = argv[1];
        to_file = argv[2];
    } else if (argc == 1) {
        from_file = "sendfile_test_file_from";
        to_file = "sendfile_test_file_to";
    } else {
        printf("Please, use ./sendfile_test <from> <to> or ./sendfile_test\n");
        return -1;
    }

    if (write_test_file(from_file)) {
        printf("Test file creating failed\n");
        return -1;
    }

    read_write_call(from_file, to_file);
    sendfile_call(from_file, to_file);

    return 0;
}
