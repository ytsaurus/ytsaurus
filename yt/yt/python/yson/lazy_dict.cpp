#include "lazy_dict.h"
#include "yson_lazy_map.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

size_t TPyObjectHasher::operator()(const Py::Object& object) const
{
    return object.hashValue();
}

TLazyDict::TLazyDict(bool alwaysCreateAttributes, const std::optional<TString>& encoding)
    : AlwaysCreateAttributes_(alwaysCreateAttributes)
    , Encoding_(encoding)
{
    Consumer_.reset(new NYTree::TPythonObjectBuilder(alwaysCreateAttributes, encoding));
}

PyObject* TLazyDict::GetItem(const Py::Object& key)
{
    auto it = Data_.find(key);
    if (it == Data_.end()) {
        Py_RETURN_NONE;
    }
    if (!it->second.Value) {
        NYson::TYsonParser parser(Consumer_.get());

        auto data = it->second.Data;
        parser.Read(TStringBuf(data.Begin(), data.Size()));
        parser.Finish();

        it->second.Value = Consumer_->ExtractObject();
    }
    return it->second.Value->ptr();
}

void TLazyDict::SetItem(const Py::Object& key, const TSharedRef& value)
{
    if (HasItem(key)) {
        Data_.erase(key);
    }
    Data_.emplace(key, TLazyDictValue({value, std::nullopt}));
}

void TLazyDict::SetItem(const Py::Object& key, const Py::Object& value)
{
    if (HasItem(key)) {
        Data_.erase(key);
    }
    Data_.emplace(key, TLazyDictValue({TSharedRef(), value}));
}

size_t TLazyDict::Length() const
{
    return Data_.size();
}

bool TLazyDict::HasItem(const Py::Object& key) const
{
    return Data_.find(key) != Data_.end();
}

void TLazyDict::DeleteItem(const Py::Object& key)
{
    Data_.erase(key);
}

void TLazyDict::Clear()
{
    Data_.clear();
}

TLazyDict::THashMapType* TLazyDict::GetUnderlyingHashMap()
{
    return &Data_;
}

Py::Object TLazyDict::GetConsumerParams()
{
    Py::Object encoding;
    if (Encoding_) {
        encoding = Py::String(*Encoding_);
    } else {
        encoding = Py::None();
    }
    return Py::TupleN(encoding, Py::Boolean(AlwaysCreateAttributes_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
