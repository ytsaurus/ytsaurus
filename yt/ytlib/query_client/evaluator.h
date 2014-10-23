#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TEvaluator);

class TEvaluator
    : public TIntrinsicRefCounted
{
public:
    explicit TEvaluator(TExecutorConfigPtr config);
    ~TEvaluator();

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

