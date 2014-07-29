#include "stdafx.h"
#include "etc_commands.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/api/client.h>

#include <core/ytree/fluent.h>

namespace NYT {
namespace NDriver {

using namespace NYPath;
using namespace NYTree;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NApi;

////////////////////////////////////////////////////////////////////////////////

void TAddMemberCommand::DoExecute()
{
    TAddMemberOptions options;
    SetMutatingOptions(&options);

    auto result = WaitFor(Context_->GetClient()->AddMember(
        Request_->Group,
        Request_->Member,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveMemberCommand::DoExecute()
{
    TRemoveMemberOptions options;
    SetMutatingOptions(&options);

    auto result = WaitFor(Context_->GetClient()->RemoveMember(
        Request_->Group,
        Request_->Member,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(result);
}

////////////////////////////////////////////////////////////////////////////////

void TParseYPathCommand::DoExecute()
{
    auto richPath = TRichYPath::Parse(Request_->Path);
    Reply(ConvertToYsonString(richPath));
}

////////////////////////////////////////////////////////////////////////////////

void TCheckPermissionCommand::DoExecute()
{
    TCheckPermissionOptions options;
    SetTransactionalOptions(&options);

    auto resultOrError = WaitFor(Context_->GetClient()->CheckPermission(
        Request_->User,
        Request_->Path.GetPath(),
        Request_->Permission,
        options));
    THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);

    const auto& result = resultOrError.Value();
    Reply(BuildYsonStringFluently()
        .BeginMap()
            .Item("action").Value(result.Action)
            .DoIf(result.ObjectId != NullObjectId, [&] (TFluentMap fluent) {
                fluent.Item("object_id").Value(result.ObjectId);
            })
            .DoIf(result.Subject.HasValue(), [&] (TFluentMap fluent) {
                fluent.Item("subject").Value(*result.Subject);
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
