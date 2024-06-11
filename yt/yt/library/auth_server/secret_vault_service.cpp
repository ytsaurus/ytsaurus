#include "secret_vault_service.h"

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(
    TStringBuilder* builder,
    const ISecretVaultService::TSecretSubrequest& subrequest,
    TStringBuf /*spec*/)
{
    builder->AppendFormat("%v:%v:%v:%v",
        subrequest.SecretId,
        subrequest.SecretVersion,
        subrequest.DelegationToken,
        subrequest.Signature,
        subrequest.TvmId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
