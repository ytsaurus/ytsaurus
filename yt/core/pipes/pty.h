#pragma once

#include "public.h"

#include <yt/core/net/public.h>

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

    NNet::IConnectionReaderPtr CreateMasterAsyncReader();
    NNet::IConnectionWriterPtr CreateMasterAsyncWriter();

    int GetMasterFD() const;
    int GetSlaveFD() const;

private:
    int MasterFD_ = InvalidFD;
    int SlaveFD_ = InvalidFD;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
