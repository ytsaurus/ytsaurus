#pragma once

#include "public.h"

#include <yt/core/misc/string.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/client/api/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T> GetByYPath(const NYTree::INodePtr& node, const NYPath::TYPath& path)
{
    try {
        auto child = NYTree::FindNodeByYPath(node, path);
        if (!child) {
            return TError("Missing %v", path);
        }
        return NYTree::ConvertTo<T>(std::move(child));
    } catch (const std::exception& ex) {
        return TError("Unable to extract %v", path) << ex;
    }
}

TString GetCryptoHash(TStringBuf secret);

////////////////////////////////////////////////////////////////////////////////

class TSafeUrlBuilder
{
public:
    void AppendString(TStringBuf str);
    void AppendChar(char ch);
    void AppendParam(TStringBuf key, TStringBuf value);

    TString FlushRealUrl();
    TString FlushSafeUrl();

private:
    TStringBuilder RealUrl_;
    TStringBuilder SafeUrl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
