#pragma once

#include "public.h"
#include "stubs.h"

#include <core/rpc/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TPrepareFacade
    : public TRefCounted
    , public IPrepareCallbacks
{
public:
    explicit TPrepareFacade(NRpc::IChannelPtr masterChannel);
    ~TPrepareFacade();

    virtual TFuture<TErrorOr<TDataSplit>> GetInitialSplit(const NYPath::TYPath& path) override;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT


