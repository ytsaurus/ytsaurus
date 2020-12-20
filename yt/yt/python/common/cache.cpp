#include "cache.h"
#include "helpers.h"

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

void TPyObjectDeleter::operator() (PyObject* object) const
{
    Py::_XDECREF(object);
}

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
    , YsonUnicode_(GetYsonTypeClass("YsonUnicode"))
    , YsonStringProxy_(GetYsonTypeClass("YsonStringProxy"))
{
    if (Encoding_) {
        EncodingObject_ = Py::String(Encoding_->data(), Encoding_->size());
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
    weight += sizeof(PyObject) + Py_SIZE(item.OriginalKey.get());

    if (Encoding_) {
        item.EncodedKey = PyObjectPtr(PyUnicode_FromEncodedObject(item.OriginalKey.get(), Encoding_->data(), "strict"));
        if (!item.EncodedKey) {
            PyErr_Clear();
            auto tuplePtr = PyObjectPtr(PyTuple_New(1));
            PyTuple_SetItem(tuplePtr.get(), 0, Py::new_reference_to(item.OriginalKey.get()));
            item.EncodedKey = PyObjectPtr(PyObject_CallObject(YsonStringProxy_.ptr(), tuplePtr.get()));
            PyObject_SetAttrString(item.EncodedKey.get(), "_encoding", EncodingObject_.ptr());
        }
        weight += sizeof(PyObject) + Py_SIZE(item.EncodedKey.get());
    }

    if (CacheEnabled_) {
        auto nonOwningKey = Py::ConvertToStringBuf(item.OriginalKey.get());
        Cache_.Insert(nonOwningKey, item, weight);
    }

    return BuildResult(item);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
