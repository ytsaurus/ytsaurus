#pragma once

#include "public.h"

#include "plan_context.h"

#include <core/misc/property.h>
#include <core/misc/guid.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TPlanFragment
{
public:
    explicit TPlanFragment(
        TPlanContextPtr context,
        const TOperator* head = nullptr,
        const TGuid& id = TGuid::Create());
    TPlanFragment(const TPlanFragment& other);
    TPlanFragment(TPlanFragment&& other);

    ~TPlanFragment();

    TPlanFragment& operator=(const TPlanFragment& other);
    TPlanFragment& operator=(TPlanFragment&& other);

    template <class TFunctor>
    void Rewrite(const TFunctor& functor)
    {
        SetHead(Apply(GetContext().Get(), GetHead(), functor));
    }

    DEFINE_BYVAL_RO_PROPERTY(TPlanContextPtr, Context);
    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Head);
    DEFINE_BYREF_RO_PROPERTY(TGuid, Id);

    static TPlanFragment Prepare(
        const Stroka& source,
        IPrepareCallbacks* callbacks);
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* serialized, const TPlanFragment& original);
TPlanFragment FromProto(const NProto::TPlanFragment& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

