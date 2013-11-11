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
    const TGuid& guid)
    : Context_(std::move(context))
    , Head_(head)
    , Guid_(guid)
{ }

TPlanFragment::TPlanFragment(const TPlanFragment& other)
    : Context_(other.Context_)
    , Head_(other.Head_)
    , Guid_(other.Guid_)
{ }

TPlanFragment::TPlanFragment(TPlanFragment&& other)
    : Context_(std::move(other.Context_))
    , Head_(std::move(other.Head_))
    , Guid_(std::move(other.Guid_))
{ }

TPlanFragment::~TPlanFragment()
{ }

TPlanFragment& TPlanFragment::operator=(const TPlanFragment& other)
{
    Context_ = other.Context_;
    Head_ = other.Head_;
    Guid_ = other.Guid_;
    return *this;
}

TPlanFragment& TPlanFragment::operator=(TPlanFragment&& other)
{
    Context_ = std::move(other.Context_);
    Head_ = std::move(other.Head_);
    Guid_ = std::move(other.Guid_);
    return *this;
}

TPlanFragment TPlanFragment::Prepare(
    const Stroka& source,
    IPrepareCallbacks* callbacks)
{
    return TPrepareController(callbacks, source).Run();
}

void ToProto(NProto::TPlanFragment* serialized, const TPlanFragment& fragment)
{
    ToProto(serialized->mutable_head(), fragment.GetHead());
    ToProto(serialized->mutable_guid(), fragment.Guid());
}

TPlanFragment FromProto(const NProto::TPlanFragment& serialized)
{
    auto context = New<TPlanContext>();
    return TPlanFragment(
        context,
        FromProto(serialized.head(), context.Get()),
        NYT::FromProto<TGuid>(serialized.guid()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

