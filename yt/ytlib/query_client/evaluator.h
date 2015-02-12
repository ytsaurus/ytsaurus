#pragma once

#include "public.h"
#include "callbacks.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
    : public TIntrinsicRefCounted
{
public:
    explicit TEvaluator(TExecutorConfigPtr config);
    ~TEvaluator();

    TQueryStatistics RunWithExecutor(
        const TConstQueryPtr& fragment,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer,
        TExecuteQuery executeCallback);

    TQueryStatistics Run(
        const TConstQueryPtr& fragment,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer);

private:
    class TImpl;

#ifdef YT_USE_LLVM
    TIntrusivePtr<TImpl> Impl_;
#endif

};

DEFINE_REFCOUNTED_TYPE(TEvaluator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

