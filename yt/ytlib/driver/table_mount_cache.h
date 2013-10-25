#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

#include <core/rpc/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TTableMountInfo
    : public TIntrinsicRefCounted
{
    NObjectClient::TObjectId TableId;
    NObjectClient::TObjectId TabletId; // NullObjectId if not mounted
    NVersionedTableClient::NProto::TTableSchemaExt Schema;
    std::vector<Stroka> KeyColumns;
};

class TTableMountCache
    : public TRefCounted
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        NRpc::IChannelPtr masterChannel);

    TFuture<TErrorOr<TTableMountInfoPtr>> LookupInfo(const NYPath::TYPath& path);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

