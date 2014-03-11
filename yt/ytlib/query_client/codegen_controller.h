#pragma once

#include "public.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TCodegenController
    : public NNonCopyable::TNonCopyable
{
public:
    TCodegenController();
    ~TCodegenController();

    TError Run(
        IEvaluateCallbacks* callbacks,
        const TPlanFragment& fragment,
        ISchemafulWriterPtr writer);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

