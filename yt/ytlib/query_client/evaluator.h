#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
{
public:
    TEvaluator();
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

