#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TPipe
{
    static const int InvalidFD;

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

class TPipeFactory
{
public:
    explicit TPipeFactory(int minFD);
    ~TPipeFactory();

    TPipe Create();

    void Clear();

private:
    int MinFD_;
    std::vector<int> ReservedFDs_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
