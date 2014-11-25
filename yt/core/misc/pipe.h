#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TPipe
{
    static const int InvalidFd;

    int ReadFd;
    int WriteFd;

    TPipe(int fd[2])
        : ReadFd(fd[0])
        , WriteFd(fd[1])
    { }

    TPipe()
        : ReadFd(-1)
        , WriteFd(-1)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
