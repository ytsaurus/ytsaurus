#pragma once

#include <util/generic/hash.h>

namespace NTvmAuth {
    class TTvmClient;
} // namespace NTvmAuth

namespace NSolomon {
    struct IAuthProvider: TThrRefBase {
        virtual void AddCredentials(THashMap<TString, TString>& headers) const = 0;
    };

    using IAuthProviderPtr = TIntrusivePtr<IAuthProvider>;

    IAuthProviderPtr CreateFakeAuth();
    IAuthProviderPtr CreateOAuthProvider(TString token);

    constexpr ui32 SOLOMON_PROD_TVM_ID{2012028};
    constexpr ui32 SOLOMON_PRE_TVM_ID{2012024};
    constexpr ui32 SOLOMON_TEST_TVM_ID{2012026};

    // You need to add cpp_client/tvm to PEERDIRs in order to use this function
    IAuthProviderPtr CreateTvmProvider(const NTvmAuth::TTvmClient& client, ui32 solomonTvmId);

} // namespace NSolomon
