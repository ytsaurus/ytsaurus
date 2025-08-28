#pragma once

#include "library/cpp/json/common/defs.h"

#include <util/generic/hash_set.h>

namespace NSJson {
    class TRootKeyFilter: public ::NJson::TJsonCallbacks {
    public:
        TRootKeyFilter(::NJson::TJsonCallbacks& nestedCallbacks, THashSet<TString> whiteList, THashSet<TString> blackList);
        bool OnNull() override;
        bool OnBoolean(bool val) override;
        bool OnInteger(long long val) override;
        bool OnUInteger(unsigned long long val) override;
        bool OnString(const TStringBuf& val) override;
        bool OnDouble(double val) override;
        bool OnOpenArray() override;
        bool OnCloseArray() override;
        bool OnOpenMap() override;
        bool OnCloseMap() override;
        bool OnMapKey(const TStringBuf& key) override;
        bool OnEnd() override;

    private:
        ::NJson::TJsonCallbacks& NestedCallbacks_;
        const THashSet<TString> WhiteList_;
        const THashSet<TString> BlackList_;
        int CurrentLevel_{};
        bool Accept_{true};
    };
}
