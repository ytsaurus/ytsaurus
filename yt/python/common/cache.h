#pragma once

#include <yt/core/misc/sync_cache.h>

#include <Objects.hxx> // pycxx

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

// NOTE: We avoid using specific PyCXX objects (e.g. Py::Bytes) to avoid unnecessary checks in hot path.

struct TPyObjectDeleter
{
    void operator() (PyObject* object) const;
};

using PyObjectPtr = std::unique_ptr<PyObject, TPyObjectDeleter>; // decltype(&Py::_XDECREF)>;

////////////////////////////////////////////////////////////////////////////////

class TPythonStringCache
{
public:
    TPythonStringCache();
    TPythonStringCache(bool enableCache, const std::optional<TString>& encoding);
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

    PyObjectPtr BuildResult(const TItem& item);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
