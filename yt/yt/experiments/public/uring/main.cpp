#include <errno.h>
#include <contrib/libs/liburing/src/include/liburing.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/eventfd.h>
#include <sys/types.h>
#include <sys/stat.h>

static void uring_read(struct io_uring* ring, int fd, int offset, int size, int user_data) {
    struct io_uring_sqe *sqe;
    struct iovec* iov = (struct iovec*)malloc(sizeof(iovec));
    void* buf = malloc(size);

    sqe = io_uring_get_sqe(ring);
    iov->iov_base = buf;
    iov->iov_len = (size_t) size;
    io_uring_prep_readv(sqe, fd, iov, 1, offset);
    sqe->user_data = user_data;
}

static void uring_submit_and_wait(struct io_uring* ring)
{
    int ret;
    struct io_uring_cqe *cqe;

    while (io_uring_sq_ready(ring) > 0) {
        ret = io_uring_submit(ring);
        printf("io_uring_submit ret %d\n", ret);
    }

    ret = io_uring_wait_cqe(ring, &cqe);

    printf("io_uring_wait_cqe ret %d\n", ret);
    printf("io_uring_wait_cqe res %d\n", cqe->res);
    printf("io_uring_wait_cqe userdata %llx\n", cqe->user_data);

    io_uring_cqe_seen(ring, cqe);
}

int main() {
    struct io_uring_params p = {};
    struct io_uring ring;

    int fd1 = eventfd(0, EFD_CLOEXEC);
    assert(fd1 >= 0);

    int fd2 = open("a.data", O_CREAT|O_RDWR, S_IRUSR | S_IWUSR);
    assert(fd2 >= 0);

    char buf[4096];
    if (write(fd2, buf, 4096) != 4096) {
        return 1;
    }

    io_uring_queue_init_params(8, &ring, &p);

    uring_read(&ring, fd1, 0, 8, 1); // XXX
    uring_read(&ring, fd2, 4096, 10, 2);
    uring_submit_and_wait(&ring);

    uring_read(&ring, fd2, 4086, 20, 3);
    uring_submit_and_wait(&ring);

    return 0;
}
