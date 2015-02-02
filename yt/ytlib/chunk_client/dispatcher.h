#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

#include <core/actions/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
    : public IShutdownable
{
public:
    TDispatcher();

    ~TDispatcher();

    static TDispatcher* Get();

    void Configure(TDispatcherConfigPtr config);

    virtual void Shutdown() override;

    /*!
     * This invoker is used for background operations in #TRemoteChunkReader
     * #TSequentialChunkReader, #TTableChunkReader and #TableReader
     */
    IInvokerPtr GetReaderInvoker();

    /*!
     * This invoker is used for background operations in
     * #TRemoteChunkWriter, #NTableClient::TChunkWriter and
     * #NTableClient::TChunkSetReader
     */
    IInvokerPtr GetWriterInvoker();

    IInvokerPtr GetCompressionPoolInvoker();

    IInvokerPtr GetErasurePoolInvoker();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

