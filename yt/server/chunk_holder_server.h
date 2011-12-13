#pragma once

#include <yt/ytlib/chunk_holder/common.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkHolderServer
{
public:
    typedef NChunkHolder::TChunkHolderConfig TConfig;

    TChunkHolderServer(
        const Stroka& configFileName,
        TConfig* config);

    void Run();

private:
    Stroka ConfigFileName;
    TConfig::TPtr Config;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
