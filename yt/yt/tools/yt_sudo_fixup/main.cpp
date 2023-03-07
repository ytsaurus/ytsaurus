#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

int main(int argc, char* argv[])
{
    if (argc < 3) {
        fprintf(stderr, "usage: yt-sudo-wrapper UID CMD [ARGS]\n");
        return 2;
    }

    int realUid = atoi(argv[1]);
    if (setreuid(realUid, 0) != 0) {
        perror("setreuid");
        return 1;
    }

    execv(argv[2], argv+3);
    perror("exec");
    return 1;
}
