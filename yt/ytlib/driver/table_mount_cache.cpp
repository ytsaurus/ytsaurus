#include "stdafx.h"
#include "table_mount_cache.h"
#include "config.h"
#include "private.h"

namespace NYT {
namespace NDriver {

using namespace NYPath;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

class TTableMountCache::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TTableMountCacheConfigPtr config,
        IChannelPtr masterChannel)
        : Config(config)
        , MasterChannel(masterChannel)
    {
        YCHECK(Config);
        YCHECK(MasterChannel);
    }

    TFuture<TErrorOr<TTableMountInfoPtr>> Lookup(const TYPath& path)
    {
        return MakeFuture(TErrorOr<TTableMountInfoPtr>(TError("oops!")));
    }

private:
    TTableMountCacheConfigPtr Config;
    IChannelPtr MasterChannel;

};

////////////////////////////////////////////////////////////////////////////////

TTableMountCache::TTableMountCache(
    TTableMountCacheConfigPtr config,
    IChannelPtr masterChannel)
    : Impl(New<TImpl>(
        config,
        masterChannel))
{ }

TFuture<TErrorOr<TTableMountInfoPtr>> TTableMountCache::Lookup(const TYPath& path)
{
    return Impl->Lookup(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

