#include "yql_ytflow_prepare.h"

#include <google/protobuf/any.pb.h>


namespace NYql::NYtflow::NPrepare::NPrivate {

class TConfigMixin
    : public virtual NYT::TRefCounted
{
public:
    void Init(TContext& prepareCtx)
    {
        RunOptions = prepareCtx.RunOptions;
    }

    TYtflowSettings::TConstPtr GetConfig() const
    {
        return RunOptions.Config();
    }

    TString GetSessionId() const
    {
        return RunOptions.SessionId();
    }

    TString GetCluster() const
    {
        auto value = GetConfig()->Cluster.Get();
        YQL_ENSURE(value, "Ytflow.Cluster pragma is not set");
        return *value;
    }

    TString GetPipelinePath() const
    {
        return GetConfig()->GetPipelinePath();
    }

private:
    IYtflowGateway::TRunOptions RunOptions;
};

using TSettingsVisitor = std::function<void (const ::google::protobuf::Any&)>;

void VisitPersistentSourceSettings(
    const TExprNode::TPtr& root,
    TContext& prepareCtx,
    const TSettingsVisitor& visitor);

void VisitPersistentSinkSettings(
    const TExprNode::TPtr& root,
    TContext& prepareCtx,
    const TSettingsVisitor& visitor);

} // namespace NYql::NYtflow::NPrepare::NPrivate
