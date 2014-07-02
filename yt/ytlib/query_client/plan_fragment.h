#pragma once

#include "public.h"

#include "plan_context.h"

#include <core/misc/property.h>
#include <core/misc/guid.h>

#include <ytlib/query_client/query_service.pb.h>

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

    template <class TFunctor>
    void Rewrite(const TFunctor& functor);

    DEFINE_BYVAL_RO_PROPERTY(TPlanContextPtr, Context);
    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Head);
    DEFINE_BYREF_RO_PROPERTY(TGuid, Id);

    static TPlanFragment Prepare(
        IPrepareCallbacks* callbacks,
        const Stroka& source,
        i64 rowLimit = std::numeric_limits<i64>::max(),
        TTimestamp timestamp = NullTimestamp);

};

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* serialized, const TPlanFragment& original);
TPlanFragment FromProto(const NProto::TPlanFragment& serialized);

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to inl
template <class TFunctor>
void TPlanFragment::Rewrite(const TFunctor& functor)
{
    SetHead(Apply(GetContext().Get(), GetHead(), functor));
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TQueryStatistics* serialized, const TQueryStatistics& original);
TQueryStatistics FromProto(const NProto::TQueryStatistics& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

