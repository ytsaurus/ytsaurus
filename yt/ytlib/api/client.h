#pragma once

#include "public.h"
#include "transaction.h"

#include <core/misc/error.h>
#include <core/misc/nullable.h>

#include <core/ytree/yson_string.h>

#include <core/rpc/public.h>

#include <ytlib/transaction_client/public.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TClientOptions
{
    TNullable<Stroka> User;
};

struct TTabletRangeOptions
{
    TNullable<int> FirstTabletIndex;
    TNullable<int> LastTabletIndex;
};

struct TMountTableOptions
    : public TTabletRangeOptions
{ };

struct TUnmountTableOptions
    : public TTabletRangeOptions
{ };

struct TReshardTableOptions
    : public TTabletRangeOptions
{ };

struct TAddMemberOptions
    : public TMutatingOptions
{ };

struct TRemoveMemberOptions
    : public TMutatingOptions
{ };

struct TCheckPermissionOptions
{ };

///////////////////////////////////////////////////////////////////////////////

struct IClient
    : public IClientBase
{
    virtual IConnectionPtr GetConnection() = 0;
    virtual NRpc::IChannelPtr GetMasterChannel() = 0;
    virtual NRpc::IChannelPtr GetSchedulerChannel() = 0;
    virtual NTransactionClient::TTransactionManagerPtr GetTransactionManager() = 0;

    // Tables
    virtual TAsyncError MountTable(
        const NYPath::TYPath& path,
        const TMountTableOptions& options = TMountTableOptions()) = 0;

    virtual TAsyncError UnmountTable(
        const NYPath::TYPath& path,
        const TUnmountTableOptions& options = TUnmountTableOptions()) = 0;

    virtual TAsyncError ReshardTable(
        const NYPath::TYPath& path,
        const std::vector<NVersionedTableClient::TKey>& pivotKeys,
        const TReshardTableOptions& options = TReshardTableOptions()) = 0;


    // Security
    virtual TAsyncError AddMember(
        const Stroka& group,
        const Stroka& member,
        const TAddMemberOptions& options = TAddMemberOptions()) = 0;

    virtual TAsyncError RemoveMember(
        const Stroka& group,
        const Stroka& member,
        const TRemoveMemberOptions& options = TRemoveMemberOptions()) = 0;

    // TODO(babenko): CheckPermission

};

IClientPtr CreateClient(
    IConnectionPtr connection,
    const TClientOptions& options);

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

