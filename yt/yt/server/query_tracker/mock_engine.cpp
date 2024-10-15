#include "mock_engine.h"

#include "handler_base.h"

#include <yt/yt/client/tablet_client/helpers.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NQueryTracker {

using namespace NYPath;
using namespace NApi;
using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TMockResult
    : public TYsonStruct
{
public:
    TError Error;
    TTableSchemaPtr Schema;
    std::vector<INodePtr> Rows;
    bool IsTruncated;

    REGISTER_YSON_STRUCT(TMockResult);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("error", &TThis::Error)
            .Default();
        registrar.Parameter("schema", &TThis::Schema)
            .Default();
        registrar.Parameter("rows", &TThis::Rows)
            .Default();
        registrar.Parameter("is_truncated", &TThis::IsTruncated)
            .Default();

        registrar.Postprocessor([] (TThis* result) {
            if (result->Error.IsOK() && !result->Schema) {
                THROW_ERROR_EXCEPTION("Either error should be non-OK or schema must be set");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TMockResult)
DECLARE_REFCOUNTED_CLASS(TMockResult)

////////////////////////////////////////////////////////////////////////////////

class TMockSettings
    : public TYsonStruct
{
public:
    TDuration Duration;

    std::vector<TMockResultPtr> Results;

    REGISTER_YSON_STRUCT(TMockSettings);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("duration", &TThis::Duration)
            .Default();
        registrar.Parameter("results", &TThis::Results)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TMockSettings)
DECLARE_REFCOUNTED_CLASS(TMockSettings)

////////////////////////////////////////////////////////////////////////////////

class TMockQueryHandler
    : public TQueryHandlerBase
{
public:
    TMockQueryHandler(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const TEngineConfigBasePtr& config,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery,
        const IInvokerPtr& controlInvoker)
        : TQueryHandlerBase(stateClient, stateRoot, controlInvoker, config, activeQuery)
        , Settings_(ConvertTo<TMockSettingsPtr>(SettingsNode_))
    { }

    void Start() override
    {
        YT_LOG_INFO("Starting mock query");
        OnQueryStarted();
        if (Query_ == "fail") {
            OnQueryFailed(TError("Mock query failed"));
        } else if (Query_ == "fail_by_exception") {
            THROW_ERROR_EXCEPTION("Query fail on start");
        } else if (Query_ == "run_forever") {
            // Just do nothing.
        } else if (Query_ == "complete_after") {
            std::vector<TErrorOr<TRowset>> rowsetOrErrors;
            for (const auto& result : Settings_->Results) {
                if (!result->Error.IsOK()) {
                    rowsetOrErrors.emplace_back(result->Error);
                } else {
                    YT_VERIFY(result->Schema);
                    TUnversionedRowsBuilder builder;

                    for (const auto& rowNode : result->Rows) {
                        auto owningRow = YsonToSchemafulRow(
                            ConvertToYsonString(rowNode).ToString(),
                            *result->Schema,
                            /*treatMissingAsNull*/ false,
                            EYsonType::Node);
                        builder.AddRow(TUnversionedRow(owningRow));
                    }
                    rowsetOrErrors.emplace_back(TRowset{.Rowset = CreateRowset(result->Schema, builder.Build()), .IsTruncated = result->IsTruncated});
                }
            }
            DelayedCookie_ = TDelayedExecutor::Submit(
                BIND(&TMockQueryHandler::OnQueryCompleted, MakeWeak(this), rowsetOrErrors).Via(GetCurrentInvoker()),
                Settings_->Duration);
        } else if (Query_ == "fail_after") {
            DelayedCookie_ = TDelayedExecutor::Submit(
                BIND(&TMockQueryHandler::OnQueryFailed, MakeWeak(this), TError("Query failed after delay")).Via(GetCurrentInvoker()),
                Settings_->Duration);
        } else {
            THROW_ERROR_EXCEPTION("Do not know what to do with mock query %Qv", Query_);
        }
    }

    void Abort() override
    {
        YT_LOG_INFO("Aborting mock query");
        TDelayedExecutor::Cancel(DelayedCookie_);
    }

    void Detach() override
    {
        YT_LOG_INFO("Detaching mock query");
        TDelayedExecutor::Cancel(DelayedCookie_);
    }

private:
    TDelayedExecutorCookie DelayedCookie_;
    TMockSettingsPtr Settings_;
};

class TMockEngine
    : public IQueryEngine
{
public:
    TMockEngine(IClientPtr stateClient, TYPath stateRoot)
        : StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
    { }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        return New<TMockQueryHandler>(StateClient_, StateRoot_, Config_, activeQuery, GetCurrentInvoker());
    }

    void Reconfigure(const TEngineConfigBasePtr& config) override
    {
        Config_ = config;
    }

private:
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    TEngineConfigBasePtr Config_;
};

IQueryEnginePtr CreateMockEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot)
{
    return New<TMockEngine>(stateClient, stateRoot);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
