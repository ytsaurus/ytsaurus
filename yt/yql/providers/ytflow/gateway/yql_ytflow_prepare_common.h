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
        YtTokenResolver = prepareCtx.YtTokenResolver;
        Credentials = prepareCtx.Credentials;
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

protected:
    const IYtTokenResolver::TPtr& GetYtTokenResolver() const
    {
        return YtTokenResolver;
    }

    const TCredentials& GetCredentials() const
    {
        return *Credentials;
    }

private:
    IYtflowGateway::TRunOptions RunOptions;
    IYtTokenResolver::TPtr YtTokenResolver;
    TCredentials::TPtr Credentials;
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
