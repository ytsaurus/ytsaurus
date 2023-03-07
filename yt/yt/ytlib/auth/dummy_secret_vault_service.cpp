#include "dummy_secret_vault_service.h"
#include "secret_vault_service.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TDummySecretVaultService
    : public ISecretVaultService
{
public:
    virtual TFuture<std::vector<TErrorOrSecretSubresponse>> GetSecrets(const std::vector<TSecretSubrequest>& subrequests) override
    {
        std::vector<TErrorOrSecretSubresponse> results;
        for (size_t index = 0; index < subrequests.size(); ++index) {
            results.push_back(TError("Secret Vault is not configured"));
        }
        return MakeFuture(std::move(results));
    }
};

ISecretVaultServicePtr CreateDummySecretVaultService()
{
    return New<TDummySecretVaultService>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
