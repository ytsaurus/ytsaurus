#include "cache.h"
#include "helpers.h"

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

TPythonStringCache::TItem::TItem() = default;

TPythonStringCache::TItem::TItem(const TItem& other)
{
    Py::_XINCREF(other.OriginalKey.get());
    OriginalKey = PyObjectPtr(other.OriginalKey.get());
    Py::_XINCREF(other.EncodedKey.get());
    EncodedKey = PyObjectPtr(other.EncodedKey.get());
}

TPythonStringCache::TPythonStringCache(bool enableCache, const std::optional<TString>& encoding)
    : CacheEnabled_(enableCache)
    , Encoding_(encoding)
    , YsonUnicode_(GetYsonTypeClass("YsonUnicode"), /* owned */ true)
{
    if (auto ysonStringProxyClass = FindYsonTypeClass("YsonStringProxy")) {
        YsonStringProxy_ = Py::Callable(ysonStringProxyClass, /* owned */ true);
    }
}

PyObjectPtr TPythonStringCache::BuildResult(const TItem& item)
{
    PyObject* result = Encoding_ ? item.EncodedKey.get() : item.OriginalKey.get();
    Py::_XINCREF(result);
    return PyObjectPtr(result);
}

PyObjectPtr TPythonStringCache::GetPythonString(TStringBuf string)
{
    if (CacheEnabled_) {
        auto keyOrNullptr = Cache_.Find(string);
        if (keyOrNullptr != nullptr) {
            return BuildResult(*keyOrNullptr);
        }
    }

    int weight = string.size();

    TItem item;
    item.OriginalKey = PyObjectPtr(PyBytes_FromStringAndSize(string.data(), string.size()));
    if (!item.OriginalKey.get()) {
        throw Py::Exception();
    }

    if (Encoding_) {
        item.EncodedKey = PyObjectPtr(PyUnicode_FromEncodedObject(item.OriginalKey.get(), Encoding_->data(), "strict"));
        if (!item.EncodedKey) {
            // COMPAT(levysotsky)
            if (!YsonStringProxy_) {
                throw Py::Exception();
            }
            PyErr_Clear();
            auto tuplePtr = PyObjectPtr(PyTuple_New(0));
            item.EncodedKey = PyObjectPtr(PyObject_CallObject(YsonStringProxy_->ptr(), tuplePtr.get()));
            if (!item.EncodedKey) {
                throw Py::Exception();
            }
            PyObject_SetAttrString(item.EncodedKey.get(), "_bytes", item.OriginalKey.get());
        }
    }

    if (CacheEnabled_) {
        auto nonOwningKey = Py::ConvertToStringBuf(item.OriginalKey.get());
        Cache_.Insert(nonOwningKey, item, weight);
    }

    return BuildResult(item);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
