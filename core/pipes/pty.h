#pragma once

#include "public.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TPty
    : public TNonCopyable
{
public:
    static const int InvalidFD = -1;

    TPty(int height, int width);
    ~TPty();

    TAsyncReaderPtr CreateMasterAsyncReader();
    TAsyncWriterPtr CreateMasterAsyncWriter();

    int GetMasterFD() const;
    int GetSlaveFD() const;

private:
    int MasterFD_ = InvalidFD;
    int SlaveFD_ = InvalidFD;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
