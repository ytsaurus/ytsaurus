#include "etc_commands.h"

#include <yt/client/api/client.h>
#include <yt/client/api/admin.h>

#include <yt/client/ypath/rich.h>

#include <yt/client/api/rpc_proxy/public.h>

#include <yt/build/build.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/async_writer.h>

namespace NYT::NDriver {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NSecurityClient;
using namespace NObjectClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NApi;
using namespace NApi::NRpcProxy;

////////////////////////////////////////////////////////////////////////////////

void TAddMemberCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->AddMember(
        Group,
        Member,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveMemberCommand::DoExecute(ICommandContextPtr context)
{
    WaitFor(context->GetClient()->RemoveMember(
        Group,
        Member,
        Options))
        .ThrowOnError();

    ProduceEmptyOutput(context);
}

////////////////////////////////////////////////////////////////////////////////

TParseYPathCommand::TParseYPathCommand()
{
    RegisterParameter("path", Path);
}

void TParseYPathCommand::DoExecute(ICommandContextPtr context)
{
    auto richPath = TRichYPath::Parse(Path);
    ProduceSingleOutputValue(context, "path", richPath);
}

////////////////////////////////////////////////////////////////////////////////

void TGetVersionCommand::DoExecute(ICommandContextPtr context)
{
    ProduceSingleOutputValue(context, "version", GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TCheckPermissionCommand::TCheckPermissionCommand()
{
    RegisterParameter("user", User);
    RegisterParameter("permission", Permission);
    RegisterParameter("path", Path);
    RegisterParameter("columns", Options.Columns)
        .Optional();
}

void TCheckPermissionCommand::DoExecute(ICommandContextPtr context)
{
    auto response =
        WaitFor(context->GetClient()->CheckPermission(
            User,
            Path.GetPath(),
            Permission,
            Options))
        .ValueOrThrow();

    auto produceResult = [] (auto fluent, const auto& result) {
        fluent
            .Item("action").Value(result.Action)
            .OptionalItem("object_id", result.ObjectId)
            .OptionalItem("object_name", result.ObjectName)
            .OptionalItem("subject_id", result.SubjectId)
            .OptionalItem("subject_name", result.SubjectName);
    };

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Do([&] (auto fluent) { produceResult(fluent, response); })
            .DoIf(response.Columns.has_value(), [&] (auto fluent) {
                fluent
                    .Item("columns").DoListFor(*response.Columns, [&] (auto fluent, const auto& result) {
                        fluent
                            .Item().BeginMap()
                                .Do([&] (auto fluent) { produceResult(fluent, result); })
                            .EndMap();
                    });
            })
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TCheckPermissionByAclCommand::TCheckPermissionByAclCommand()
{
    RegisterParameter("user", User);
    RegisterParameter("permission", Permission);
    RegisterParameter("acl", Acl);
}

void TCheckPermissionByAclCommand::DoExecute(ICommandContextPtr context)
{
    auto result =
        WaitFor(context->GetClient()->CheckPermissionByAcl(
            User,
            Permission,
            Acl,
            Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("action").Value(result.Action)
            .OptionalItem("subject_id", result.SubjectId)
            .OptionalItem("subject_name", result.SubjectName)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TExecuteBatchCommand::TRequest::TRequest()
{
    RegisterParameter("command", Command);
    RegisterParameter("parameters", Parameters);
    RegisterParameter("input", Input)
        .Default();
}

class TExecuteBatchCommand::TRequestExecutor
    : public TIntrinsicRefCounted
{
public:
    TRequestExecutor(
        ICommandContextPtr context,
        TRequestPtr request,
        NRpc::TMutationId mutationId,
        bool retry)
        : Context_(std::move(context))
        , Request_(std::move(request))
        , MutationId_(mutationId)
        , Retry_(retry)
        , SyncInput_(Input_)
        , AsyncInput_(CreateAsyncAdapter(
            &SyncInput_,
            Context_->GetClient()->GetConnection()->GetInvoker()))
        , SyncOutput_(Output_)
        , AsyncOutput_(CreateAsyncAdapter(
            &SyncOutput_,
            Context_->GetClient()->GetConnection()->GetInvoker()))
    { }

    TFuture<TYsonString> Run()
    {
        auto driver = Context_->GetDriver();
        Descriptor_ = driver->GetCommandDescriptorOrThrow(Request_->Command);

        if (Descriptor_.InputType != EDataType::Null &&
            Descriptor_.InputType != EDataType::Structured)
        {
            THROW_ERROR_EXCEPTION("Command %Qv cannot be part of a batch since it has inappropriate input type %Qlv",
                Request_->Command,
                Descriptor_.InputType);
        }

        if (Descriptor_.OutputType != EDataType::Null &&
            Descriptor_.OutputType != EDataType::Structured)
        {
            THROW_ERROR_EXCEPTION("Command %Qv cannot be part of a batch since it has inappropriate output type %Qlv",
                Request_->Command,
                Descriptor_.OutputType);
        }

        TDriverRequest driverRequest;
        driverRequest.Id = Context_->Request().Id;
        driverRequest.CommandName = Request_->Command;
        auto parameters = IAttributeDictionary::FromMap(Request_->Parameters);
        if (Descriptor_.InputType == EDataType::Structured) {
            if (!Request_->Input) {
                THROW_ERROR_EXCEPTION("Command %Qv requires input",
                    Descriptor_.CommandName);
            }
            Input_ = ConvertToYsonString(Request_->Input).GetData();
            parameters->Set("input_format", TFormat(EFormatType::Yson));
            driverRequest.InputStream = AsyncInput_;
        }
        if (Descriptor_.OutputType == EDataType::Structured) {
            parameters->Set("output_format", TFormat(EFormatType::Yson));
            driverRequest.OutputStream = AsyncOutput_;
        }
        if (Descriptor_.Volatile) {
            // XXX(babenko): investigate 'Duplicate request is not marked as "retry"'
            const auto& Logger = DriverLogger;
            YT_LOG_DEBUG("XXX %v", MutationId_);
            parameters->Set("mutation_id", MutationId_);
            parameters->Set("retry", Retry_);
        }
        driverRequest.Parameters = parameters->ToMap();
        driverRequest.AuthenticatedUser = Context_->Request().AuthenticatedUser;

        return driver->Execute(driverRequest).Apply(
            BIND(&TRequestExecutor::OnResponse, MakeStrong(this)));
    }

private:
    const ICommandContextPtr Context_;
    const TRequestPtr Request_;
    const NRpc::TMutationId MutationId_;
    const bool Retry_;

    TCommandDescriptor Descriptor_;

    TString Input_;
    TStringInput SyncInput_;
    IAsyncInputStreamPtr AsyncInput_;

    TString Output_;
    TStringOutput SyncOutput_;
    IFlushableAsyncOutputStreamPtr AsyncOutput_;

    TYsonString OnResponse(const TError& error)
    {
        return BuildYsonStringFluently()
            .BeginMap()
                .DoIf(!error.IsOK(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("error").Value(error);
                })
                .DoIf(error.IsOK() && Descriptor_.OutputType == EDataType::Structured, [&] (TFluentMap fluent) {
                    fluent
                        .Item("output").Value(TYsonString(Output_));
                })
            .EndMap();
    }
};

TExecuteBatchCommand::TExecuteBatchCommand()
{
    RegisterParameter("concurrency", Options.Concurrency)
        .Default(50)
        .GreaterThan(0);
    RegisterParameter("requests", Requests);
}

void TExecuteBatchCommand::DoExecute(ICommandContextPtr context)
{
    auto mutationId = Options.GetOrGenerateMutationId();

    std::vector<TCallback<TFuture<TYsonString>()>> callbacks;
    for (const auto& request : Requests) {
        auto executor = New<TRequestExecutor>(
            context,
            request,
            mutationId,
            Options.Retry);
        mutationId = NRpc::GenerateNextBatchMutationId(mutationId);
        callbacks.push_back(BIND(&TRequestExecutor::Run, executor));
    }

    auto results = WaitFor(RunWithBoundedConcurrency(callbacks, Options.Concurrency))
        .ValueOrThrow();

    ProduceSingleOutput(context, "results", [&] (NYson::IYsonConsumer* consumer) {
        BuildYsonFluently(consumer)
            .DoListFor(results, [&] (TFluentList fluent, const TErrorOr<TYsonString>& result) {
                fluent.Item().Value(result.ValueOrThrow());
            });
    });
}

////////////////////////////////////////////////////////////////////////////////

TDiscoverProxiesCommand::TDiscoverProxiesCommand()
{
    RegisterParameter("type", Type)
        .Default(EProxyType::Rpc);
    RegisterParameter("role", Role)
        .Default(NRpcProxy::DefaultProxyRole);
}

void TDiscoverProxiesCommand::DoExecute(ICommandContextPtr context)
{
    if (Type != EProxyType::Rpc && Type != EProxyType::Grpc) {
        THROW_ERROR_EXCEPTION("Proxy type is not supported")
            << TErrorAttribute("proxy_type", Type);
    }

    TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;
    options.Attributes = {BannedAttributeName, RoleAttributeName};

    TString path = (Type == EProxyType::Rpc) ? RpcProxiesPath : GrpcProxiesPath;

    auto nodesYson = WaitFor(context->GetClient()->GetNode(path, options))
        .ValueOrThrow();

    std::vector<TString> addresses;
    for (const auto& proxy : ConvertTo<THashMap<TString, IMapNodePtr>>(nodesYson)) {
        if (!proxy.second->FindChild(AliveNodeName)) {
            continue;
        }

        if (proxy.second->Attributes().Get(BannedAttributeName, false)) {
            continue;
        }

        if (Role && proxy.second->Attributes().Get<TString>(RoleAttributeName, DefaultProxyRole) != *Role) {
            continue;
        }

        addresses.push_back(proxy.first);
    }

    ProduceSingleOutputValue(context, "proxies", addresses);
}

////////////////////////////////////////////////////////////////////////////////

TBalanceTabletCellsCommand::TBalanceTabletCellsCommand()
{
    RegisterParameter("bundle", TabletCellBundle);
    RegisterParameter("tables", MovableTables)
        .Optional();
    RegisterParameter("keep_actions", Options.KeepActions)
        .Default(false);
}

void TBalanceTabletCellsCommand::DoExecute(ICommandContextPtr context)
{
    auto asyncResult = context->GetClient()->BalanceTabletCells(
        TabletCellBundle,
        MovableTables,
        Options);
    auto tabletActions = WaitFor(asyncResult)
        .ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently().List(tabletActions));
}

////////////////////////////////////////////////////////////////////////////////

TBuildSnapshotCommand::TBuildSnapshotCommand()
{
    RegisterParameter("cell_id", Options.CellId);

    RegisterParameter("set_read_only", Options.SetReadOnly)
        .Optional();
    RegisterParameter("wait_for_snapshot_completion", Options.WaitForSnapshotCompletion)
        .Optional();
}

void TBuildSnapshotCommand::DoExecute(ICommandContextPtr context)
{
    if (!ValidateSuperuserPermissions(context)) {
        THROW_ERROR_EXCEPTION("User not authorized");
    }

    auto admin = context->GetDriver()->GetConnection()->CreateAdmin(TAdminOptions{});
    auto snapshotIdOrError = WaitFor(admin->BuildSnapshot(Options));
    auto snapshotId = snapshotIdOrError.ValueOrThrow();
    context->ProduceOutputValue(BuildYsonStringFluently()
        .BeginMap()
            .Item("snapshot_id").Value(snapshotId)
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TBuildMasterSnapshotsCommand::TBuildMasterSnapshotsCommand()
{
    RegisterParameter("set_read_only", Options.SetReadOnly)
        .Optional();
    RegisterParameter("wait_for_snapshot_completion", Options.WaitForSnapshotCompletion)
        .Optional();
    RegisterParameter("retry", Options.Retry)
        .Optional();
}

void TBuildMasterSnapshotsCommand::DoExecute(ICommandContextPtr context)
{
    if (!ValidateSuperuserPermissions(context)) {
        THROW_ERROR_EXCEPTION("User not authorized");
    }

    auto admin = context->GetDriver()->GetConnection()->CreateAdmin(TAdminOptions{});
    auto cellIdToSnapshotId = WaitFor(admin->BuildMasterSnapshots(Options))
        .ValueOrThrow();

    context->ProduceOutputValue(BuildYsonStringFluently()
        .DoListFor(cellIdToSnapshotId, [=] (TFluentList fluent, const auto& pair) {
            fluent
                .Item().BeginMap()
                    .Item("cell_id").Value(pair.first)
                    .Item("snapshot_id").Value(pair.second)
                .EndMap();
        })
    );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
