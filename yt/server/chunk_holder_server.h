#pragma once

#include <yt/ytlib/chunk_holder/chunk_holder_service.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServer
{
public:
    typedef NChunkHolder::TChunkHolderConfig TConfig;

    TChunkHolderServer(const TConfig& config);
    void Run();

private:
    TConfig Config;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
