#include "filter.h"

namespace NSJson {
    TRootKeyFilter::TRootKeyFilter(::NJson::TJsonCallbacks& nestedCallbacks, THashSet<TString> whiteList, THashSet<TString> blackList)
        : TJsonCallbacks(true)
        , NestedCallbacks_(nestedCallbacks)
        , WhiteList_(std::move(whiteList))
        , BlackList_(std::move(blackList))
    {
        Y_ASSERT(WhiteList_.empty() || BlackList_.empty());
    }

    bool TRootKeyFilter::OnNull() {
        if (Accept_) {
            return NestedCallbacks_.OnNull();
        }
        return true;
    }

    bool TRootKeyFilter::OnBoolean(bool val) {
        if (Accept_) {
            return NestedCallbacks_.OnBoolean(val);
        }
        return true;
    }

    bool TRootKeyFilter::OnInteger(long long val) {
        if (Accept_) {
            return NestedCallbacks_.OnInteger(val);
        }
        return true;
    }

    bool TRootKeyFilter::OnUInteger(unsigned long long val) {
        if (Accept_) {
            return NestedCallbacks_.OnUInteger(val);
        }
        return true;
    }

    bool TRootKeyFilter::OnString(const TStringBuf& val) {
        if (Accept_) {
            return NestedCallbacks_.OnString(val);
        }
        return true;
    }

    bool TRootKeyFilter::OnDouble(double val) {
        if (Accept_) {
            return NestedCallbacks_.OnDouble(val);
        }
        return true;
    }

    bool TRootKeyFilter::OnOpenArray() {
        ++CurrentLevel_;
        if (Accept_) {
            return NestedCallbacks_.OnOpenArray();
        }
        return true;
    }

    bool TRootKeyFilter::OnCloseArray() {
        --CurrentLevel_;
        if (Accept_) {
            return NestedCallbacks_.OnCloseArray();
        }
        return true;
    }

    bool TRootKeyFilter::OnOpenMap() {
        ++CurrentLevel_;
        if (Accept_) {
            return NestedCallbacks_.OnOpenMap();
        }
        return true;
    }

    bool TRootKeyFilter::OnCloseMap() {
        --CurrentLevel_;
        if (Accept_ || CurrentLevel_ == 0) {
            return NestedCallbacks_.OnCloseMap();
        }
        return true;
    }

    bool TRootKeyFilter::OnMapKey(const TStringBuf& key) {
        if (CurrentLevel_ == 1) {
            if (WhiteList_) {
                Accept_ = WhiteList_.contains(key);
            } else if (BlackList_) {
                Accept_ = !BlackList_.contains(key);
            }
        }
        if (Accept_) {
            return NestedCallbacks_.OnMapKey(key);
        }
        return true;
    }

    bool TRootKeyFilter::OnEnd() {
        return NestedCallbacks_.OnEnd();
    }
}
