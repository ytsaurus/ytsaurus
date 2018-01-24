#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/misc/ref.h>

#include <yt/server/data_node/public.h>

#include <util/system/file.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IIOEngine
    : public TRefCounted
{
    virtual ~IIOEngine() = default;

    virtual TFuture<TSharedMutableRef> Pread(const std::shared_ptr<TFileHandle>& fh, size_t len, i64 offset) = 0;
    virtual TFuture<void> Pwrite(const std::shared_ptr<TFileHandle>& fh, const TSharedMutableRef& data, i64 offset) = 0;

    virtual TFuture<bool> FlushData(const std::shared_ptr<TFileHandle>& fh) = 0;
    virtual TFuture<bool> Flush(const std::shared_ptr<TFileHandle>& fh) = 0;

    virtual std::shared_ptr<TFileHandle> Open(const TString& fName, EOpenMode oMode) = 0;
};

DEFINE_REFCOUNTED_TYPE(IIOEngine)

IIOEnginePtr CreateIOEngine(NDataNode::EIOEngineType ioType, const NYTree::INodePtr& ioConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
