#include "stdafx.h"
#include "etc_commands.h"

#include <core/build.h>
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

void TAddMemberCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AddMember(
        Group,
        Member,
        Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveMemberCommand::Execute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RemoveMember(
        Group,
        Member,
        Options))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TParseYPathCommand::Execute(ICommandContextPtr context)
{
    auto richPath = TRichYPath::Parse(Path);
    context->ProduceOutputValue(ConvertToYsonString(richPath));
}

////////////////////////////////////////////////////////////////////////////////

void TGetVersionCommand::Execute(ICommandContextPtr context)
{
    context->ProduceOutputValue(ConvertToYsonString(GetVersion()));
}

////////////////////////////////////////////////////////////////////////////////

void TCheckPermissionCommand::Execute(ICommandContextPtr context)
{
    auto result = WaitFor(context->GetClient()->CheckPermission(
        User,
        Path.GetPath(),
        Permission,
        Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("action").Value(result.Action)
            .DoIf(result.ObjectId.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("object_id").Value(result.ObjectId);
            })
            .DoIf(result.ObjectName.HasValue(), [&] (TFluentMap fluent) {
                fluent.Item("object_name").Value(result.ObjectName);
            })
            .DoIf(result.SubjectId.operator bool(), [&] (TFluentMap fluent) {
                fluent.Item("subject_id").Value(result.SubjectId);
            })
            .DoIf(result.SubjectName.HasValue(), [&] (TFluentMap fluent) {
                fluent.Item("subject_name").Value(result.SubjectName);
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
