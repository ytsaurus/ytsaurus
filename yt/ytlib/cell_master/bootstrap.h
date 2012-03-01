#pragma once

#include "public.h"

#include <ytlib/transaction_server/public.h>

#include <ytlib/misc/property.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(
        const Stroka& configFileName,
        TCellMasterConfig* config);

    ~TBootstrap();

    NTransactionServer::TTransactionManager* GetTransactionManager() const;

    void Run();

private:
    Stroka ConfigFileName;
    TCellMasterConfigPtr Config;

    NTransactionServer::TTransactionManagerPtr TransactionManager;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
