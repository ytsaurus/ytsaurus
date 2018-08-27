#include "clique_authorization_manager.h"

#include "private.h"

#include <yt/ytlib/scheduler/helpers.h>

namespace NYT {
namespace NClickHouse {

using namespace NApi;
using namespace NYTree;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TCliqueAuthorizationManager
    : public NInterop::ICliqueAuthorizationManager
{
public:
    TCliqueAuthorizationManager(
        IClientPtr client,
        TString cliqueId,
        bool validateOperationPermission)
        : Client_(std::move(client))
        , CliqueId_(std::move(cliqueId))
        , ValidateOperationPermission_(validateOperationPermission)
    { }

    virtual bool HasAccess(const std::string& user) override
    {
        if (!ValidateOperationPermission_) {
            return true;
        }

        try {
            NScheduler::ValidateOperationPermission(
                TString(user),
                TOperationId::FromString(CliqueId_),
                Client_,
                EPermission::Write,
                Logger);
            return true;
        } catch (const std::exception& ex) {
            LOG_INFO(ex, "User does not have access to the containing operation (User: %v, OperationId: %v)",
                user,
                CliqueId_);
            return false;
        }
    }

private:
    IClientPtr Client_;
    TString CliqueId_;
    bool ValidateOperationPermission_ = false;
    const NLogging::TLogger& Logger = ServerLogger;
};

NInterop::ICliqueAuthorizationManagerPtr CreateCliqueAuthorizationManager(
    IClientPtr client,
    TString cliqueId,
    bool validateOperationPermission)
{
    return std::make_shared<TCliqueAuthorizationManager>(std::move(client), std::move(cliqueId), validateOperationPermission);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NClickHouse
}   // namespace NYT
