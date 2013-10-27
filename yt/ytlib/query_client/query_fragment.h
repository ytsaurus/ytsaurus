#pragma once

#include "public.h"

#include "query_context.h"

#include <core/misc/property.h>
#include <core/misc/guid.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TQueryFragment
{
public:
    explicit TQueryFragment(
        TQueryContextPtr context,
        const TOperator* head = nullptr,
        const TGuid& guid = TGuid::Create());

    TQueryFragment(const TQueryFragment& other);

    TQueryFragment(TQueryFragment&& other);

    ~TQueryFragment();

    DEFINE_BYVAL_RO_PROPERTY(TQueryContextPtr, Context);
    DEFINE_BYVAL_RW_PROPERTY(const TOperator*, Head);
    DEFINE_BYREF_RO_PROPERTY(TGuid, Guid);

};

TQueryFragment PrepareQueryFragment(
    IPrepareCallbacks* callbacks,
    const Stroka& source);

////////////////////////////////////////////////////////////////////////////////

namespace NProto { class TQueryFragment; }
void ToProto(NProto::TQueryFragment* serialized, const TQueryFragment& original);
TQueryFragment FromProto(const NProto::TQueryFragment& serialized);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

