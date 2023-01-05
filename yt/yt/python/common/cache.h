#pragma once

#include "helpers.h"

#include <yt/yt/core/misc/sync_cache.h>

#include <CXX/Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TPythonStringCache
{
public:
    TPythonStringCache(bool enableCache = false, const std::optional<TString>& encoding = {});
    TPythonStringCache& operator=(const TPythonStringCache& other) = default;
    PyObjectPtr GetPythonString(TStringBuf string);

private:
    struct TItem
    {
        PyObjectPtr OriginalKey;
        PyObjectPtr EncodedKey;

        TItem();
        TItem(const TItem& other);
    };

    bool CacheEnabled_ = false;
    std::optional<TString> Encoding_;
    using TCache = TSimpleLruCache<TStringBuf, TItem>;
    TCache Cache_ = TCache(1_MB);
    Py::Callable YsonUnicode_;
    std::optional<Py::Callable> YsonStringProxy_;

    PyObjectPtr BuildResult(const TItem& item);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
