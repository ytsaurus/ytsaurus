#pragma once

#include "public.h"

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellMasterConfig* config);

    void Run();

private:
    Stroka ConfigFileName;
    TCellMasterConfigPtr Config;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
