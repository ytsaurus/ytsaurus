#include "credential_provider.h"

#include <yt/yt/library/tvm/service/config.h>
#include <yt/yt/library/tvm/service/tvm_service.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////////

class TStaticCredentialProvider
    : public ICredentialsProvider
{
public:
    TStaticCredentialProvider(TString accessKey, TString secretKey)
        : Credentials_(TCredentials{.AccessKeyId = std::move(accessKey), .SecretAccessKey = std::move(secretKey)})
    {
    }

    TCredentials GetCredentials() override
    {
        return Credentials_;
    }

private:
    const TCredentials Credentials_;
};

////////////////////////////////////////////////////////////////////////////////

class TTvmCredentialProvider
    : public ICredentialsProvider
{
public:
    TTvmCredentialProvider(NAuth::TTvmId selfTvm, NAuth::TTvmId s3Tvm, TString tvmSecret)
        : SelfTvm_(selfTvm)
        , S3Tvm_(s3Tvm)
        , TvmService_(BuildTvmService(selfTvm, s3Tvm, std::move(tvmSecret)))
    {
    }

    TCredentials GetCredentials() override
    {
        return TCredentials{
            .AccessKeyId = ToString(SelfTvm_), // Pass TVM ID in AccessKeyId for better authentification debugging.
            .SessionToken = TvmService_->GetServiceTicket(S3Tvm_),
        };
    }

private:
    static NAuth::ITvmServicePtr BuildTvmService(NAuth::TTvmId selfTvm, NAuth::TTvmId s3Tvm, TString tvmSecret)
    {
        auto config = New<NAuth::TTvmServiceConfig>();
        config->ClientSelfId = selfTvm;
        config->ClientSelfSecret = std::move(tvmSecret);
        config->ClientDstMap["S3"] = s3Tvm;
        config->ClientEnableServiceTicketFetching = true;
        return NAuth::CreateTvmService(std::move(config));
    }

private:
    const NAuth::TTvmId SelfTvm_;
    const NAuth::TTvmId S3Tvm_;
    const NAuth::ITvmServicePtr TvmService_;
};

////////////////////////////////////////////////////////////////////////////////

ICredentialsProviderPtr CreateAnonymousCredentialProvider()
{
    return CreateStaticCredentialProvider(/*accessKey*/ "", /*secretKey*/ "");
}

ICredentialsProviderPtr CreateStaticCredentialProvider(TString accessKey, TString secretKey)
{
    return New<TStaticCredentialProvider>(std::move(accessKey), std::move(secretKey));
}

ICredentialsProviderPtr CreateTVMCredentialProvider(NAuth::TTvmId selfTvm, NAuth::TTvmId s3Tvm, TString tvmSecret)
{
    return New<TTvmCredentialProvider>(selfTvm, s3Tvm, std::move(tvmSecret));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
