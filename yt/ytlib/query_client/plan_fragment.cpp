#include "stdafx.h"
#include "plan_fragment.h"

#include "prepare_controller.h"

#include "plan_node.h"

#include <ytlib/query_client/plan_fragment.pb.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TPlanFragment::TPlanFragment(
    TPlanContextPtr context,
    const TOperator* head,
    const TGuid& id)
    : Context_(std::move(context))
    , Head_(head)
    , Id_(id)
{ }

TPlanFragment::TPlanFragment(const TPlanFragment& other)
    : Context_(other.Context_)
    , Head_(other.Head_)
    , Id_(other.Id_)
{ }

TPlanFragment::TPlanFragment(TPlanFragment&& other)
    : Context_(std::move(other.Context_))
    , Head_(std::move(other.Head_))
    , Id_(std::move(other.Id_))
{ }

TPlanFragment::~TPlanFragment()
{ }

TPlanFragment& TPlanFragment::operator=(const TPlanFragment& other)
{
    Context_ = other.Context_;
    Head_ = other.Head_;
    Id_ = other.Id_;
    return *this;
}

TPlanFragment& TPlanFragment::operator=(TPlanFragment&& other)
{
    Context_ = std::move(other.Context_);
    Head_ = std::move(other.Head_);
    Id_ = std::move(other.Id_);
    return *this;
}

TPlanFragment TPlanFragment::Prepare(
    const Stroka& source,
    TTimestamp timestamp,
    IPrepareCallbacks* callbacks)
{
    return TPrepareController(callbacks, timestamp, source).Run();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TPlanFragment* serialized, const TPlanFragment& fragment)
{
    ToProto(serialized->mutable_head(), fragment.GetHead());
    ToProto(serialized->mutable_id(), fragment.Id());
    serialized->set_timestamp(fragment.GetContext()->GetTimestamp());
}

TPlanFragment FromProto(const NProto::TPlanFragment& serialized)
{
    auto context = New<TPlanContext>(serialized.timestamp());
    return TPlanFragment(
        context,
        FromProto(serialized.head(), context.Get()),
        NYT::FromProto<TGuid>(serialized.id()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

