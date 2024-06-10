#include "auth.h"

#include <util/string/builder.h>


namespace NSolomon {
namespace {
    struct TFakeAuthProvider: IAuthProvider {
        void AddCredentials(THashMap<TString, TString>&) const override {
        }
    };

    struct TOAuthProvider: IAuthProvider {
        TOAuthProvider(TString token)
            : Token_{std::move(token)}
        {
        }

        void AddCredentials(THashMap<TString, TString>& headers) const override {
            headers["Authorization"] = TStringBuilder() << "OAuth " << Token_;
        }

    private:
        TString Token_;
    };

} // namespace

    IAuthProviderPtr CreateFakeAuth() {
        return new TFakeAuthProvider;
    }

    IAuthProviderPtr CreateOAuthProvider(TString token) {
        return new TOAuthProvider{std::move(token)};
    }
} // namespace NSolomon
