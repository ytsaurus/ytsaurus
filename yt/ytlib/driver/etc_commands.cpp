#include "stdafx.h"
#include "etc_commands.h"

#include <core/concurrency/fiber.h>

#include <core/ypath/token.h>

#include <core/ytree/convert.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/security_client/group_ypath_proxy.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYPath;
using namespace NYTree;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka GetGroupPath(const Stroka& name)
{
    return "//sys/groups/" + ToYPathLiteral(name);
}

} // namespace

void TAddMemberCommand::DoExecute()
{
    auto req = TGroupYPathProxy::AddMember(GetGroupPath(Request->Group));
    req->set_name(Request->Member);
    GenerateMutationId(req);

    auto rsp = WaitFor(ObjectProxy->Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveMemberCommand::DoExecute()
{
    auto req = TGroupYPathProxy::RemoveMember(GetGroupPath(Request->Group));
    req->set_name(Request->Member);
    GenerateMutationId(req);

    auto rsp = WaitFor(ObjectProxy->Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
}

////////////////////////////////////////////////////////////////////////////////

void TParseYPathCommand::DoExecute()
{
    auto richPath = TRichYPath::Parse(Request->Path);
    ReplySuccess(ConvertToYsonString(richPath));
}

////////////////////////////////////////////////////////////////////////////////

void TCheckPersmissionCommand::DoExecute()
{
    auto req = TObjectYPathProxy::CheckPermission(Request->Path.GetPath());
    req->set_user(Request->User);
    req->set_permission(Request->Permission);
    SetTransactionId(req, EAllowNullTransaction::Yes);

    auto rsp = WaitFor(ObjectProxy->Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);

    ReplySuccess(BuildYsonStringFluently()
        .BeginMap()
            .Item("action").Value(ESecurityAction(rsp->action()))
            .DoIf(rsp->has_object_id(), [&] (TFluentMap fluent) {
                fluent.Item("object_id").Value(FromProto<TObjectId>(rsp->object_id()));
            })
            .DoIf(rsp->has_subject(), [&] (TFluentMap fluent) {
                fluent.Item("subject").Value(rsp->subject());
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
