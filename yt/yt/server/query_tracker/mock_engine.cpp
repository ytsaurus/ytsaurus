#include "mock_engine.h"

#include "handler_base.h"

namespace NYT::NQueryTracker {

using namespace NYPath;
using namespace NApi;

///////////////////////////////////////////////////////////////////////////////

class TMockQueryHandler
    : public TQueryHandlerBase
{
public:
    TMockQueryHandler(
        const NApi::IClientPtr& stateClient,
        const NYPath::TYPath& stateRoot,
        const TEngineConfigBasePtr& config,
        const NQueryTrackerClient::NRecords::TActiveQuery& activeQuery)
        : TQueryHandlerBase(stateClient, stateRoot, config, activeQuery)
    { }

    void Start() override
    {
        YT_LOG_INFO("Starting mock query");
        if (Query_ == "fail") {
            OnQueryFailed(TError("Mock query failed"));
        } else if (Query_ == "fail_on_start") {
            THROW_ERROR_EXCEPTION("Query fail on start");
        } else if (Query_ == "run_forever") {
            // Just do nothing.
        } else if (Query_.StartsWith("run_for")) {
            // XXX
        } else {
            THROW_ERROR_EXCEPTION("Do not know what to do with mock query %Qv", Query_);
        }
    }

    void Abort() override
    {
        YT_LOG_INFO("Aborting mock query");
    }

    void Detach() override
    {
        YT_LOG_INFO("Detaching mock query");
    }
};

class TMockEngine
    : public IQueryEngine
{
public:
    TMockEngine(const IClientPtr& stateClient, const TYPath& stateRoot)
        : StateClient_(stateClient)
        , StateRoot_(stateRoot)
    { }

    IQueryHandlerPtr StartOrAttachQuery(NRecords::TActiveQuery activeQuery) override
    {
        return New<TMockQueryHandler>(StateClient_, StateRoot_, Config_, activeQuery);
    }

    void OnDynamicConfigChanged(const TEngineConfigBasePtr& config) override
    {
        Config_ = config;
    }

private:
    IClientPtr StateClient_;
    TYPath StateRoot_;
    TEngineConfigBasePtr Config_;
};

IQueryEnginePtr CreateMockEngine(const NApi::IClientPtr& stateClient, const NYPath::TYPath& stateRoot)
{
    return New<TMockEngine>(stateClient, stateRoot);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
