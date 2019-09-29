#include "annotation_setter.h"
#include "bootstrap.h"
#include "multicell_manager.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/ypath/token.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NYPath;
using namespace NApi;

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TAnnotationSetter::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    {
        const auto& connection = Bootstrap_->GetClusterConnection();
        Client_ = connection->CreateClient(TClientOptions(NSecurityClient::RootUserName));

        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::SetAnnotations, MakeWeak(this)),
            Bootstrap_->GetConfig()->AnnotationSetterPeriod);

        auto address = Format("%v:%v",
            Bootstrap_->GetConfig()->AddressResolver->LocalHostFqdn,
            Bootstrap_->GetConfig()->RpcPort);

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            Path_ = Format("//sys/primary_masters/%v", ToYPathLiteral(address));
        } else {
            Path_ = Format("//sys/secondary_masters/%v/%v",
                ToYPathLiteral(multicellManager->GetCellTag()),
                ToYPathLiteral(address));
        }
    }

    void Start()
    {
        YT_LOG_DEBUG("Starting Cypress annotations setter");
        PeriodicExecutor_->Start();
    }

private:
    const TBootstrap* const Bootstrap_;
    TString Path_;
    TPeriodicExecutorPtr PeriodicExecutor_;
    IClientPtr Client_;

    void SetAnnotations()
    {
        auto annotations = ConvertToYsonString(Bootstrap_->GetConfig()->CypressAnnotations);
        auto error = WaitFor(Client_->SetNode(Path_ + "/@annotations", annotations));

        if (error.IsOK()) {
            YT_LOG_DEBUG("Successfully set Cypress annotations");
            PeriodicExecutor_->Stop();
        } else {
            YT_LOG_DEBUG(error, "Failed to set Cypress annotations");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TAnnotationSetter::TAnnotationSetter(
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

void TAnnotationSetter::Start()
{
    Impl_->Start();
}

TAnnotationSetter::~TAnnotationSetter()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
