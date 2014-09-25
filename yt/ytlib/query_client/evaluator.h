#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
{
public:
    explicit TEvaluator(TExecutorConfigPtr config);
    ~TEvaluator();

    TQueryStatistics Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

