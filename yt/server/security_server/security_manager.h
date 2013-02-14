#pragma once

#include "public.h"
#include "cluster_resources.h"

#include <ytlib/meta_state/map.h>

#include <ytlib/rpc/service.h>

#include <server/cell_master/public.h>

#include <server/cypress_server/public.h>

#include <server/transaction_server/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TSecurityManager
    : public TRefCounted
{
public:
    explicit TSecurityManager(NCellMaster::TBootstrap* bootstrap);
    ~TSecurityManager();

    void Initialize();

    DECLARE_METAMAP_ACCESSORS(Account, TAccount, TAccountId);


    //! Returns an account with a given name (|nullptr| if none).
    TAccount* FindAccountByName(const Stroka& name);

    //! Returns the "sys" built-in account.
    TAccount* GetSysAccount();

    //! Returns the "tmp" built-in account.
    TAccount* GetTmpAccount();


    //! Assigns node to a given account, updates the total resource usage.
    void SetAccount(NCypressServer::TCypressNodeBase* node, TAccount* account);

    //! Removes account association (if any) from the node.
    void ResetAccount(NCypressServer::TCypressNodeBase* node);


    //! Updates the account to accommodate recent changes in #node resource usage.
    void UpdateAccountNodeUsage(NCypressServer::TCypressNodeBase* node);

    //! Updates the staging resource usage for a given account.
    void UpdateAccountStagingUsage(
        NTransactionServer::TTransaction* transaction,
        TAccount* account,
        const TClusterResources& delta);

private:
    class TImpl;
    class TAccountTypeHandler;

    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
