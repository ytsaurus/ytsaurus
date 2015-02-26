#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TPipe
{
    static const int InvalidFd;

    int ReadFD;
    int WriteFD;

    TPipe(int fd[2])
        : ReadFD(fd[0])
        , WriteFD(fd[1])
    { }

    TPipe()
        : ReadFD(-1)
        , WriteFD(-1)
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
