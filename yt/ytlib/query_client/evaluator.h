#pragma once

#include "public.h"
#include "callbacks.h"
#include "function_registry.h"

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
        TConstQueryPtr fragment,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer,
        TExecuteQuery executeCallback,
        const IFunctionRegistryPtr functionRegistry,
        bool enableCodeCache);

    TQueryStatistics Run(
        TConstQueryPtr fragment,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer,
        const IFunctionRegistryPtr functionRegistry,
        bool enableCodeCache);

private:
    class TImpl;

    TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TEvaluator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

