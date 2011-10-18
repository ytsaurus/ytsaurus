#pragma once

#include <yt/ytlib/chunk_holder/chunk_holder.h>

namespace NYT {

using NChunkHolder::TChunkHolderConfig;
using NChunkHolder::TChunkHolder;

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServer
{
public:
    typedef TChunkHolderConfig TConfig;

    TChunkHolderServer(const TConfig& config);
    void Run();

private:
    TConfig Config;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
