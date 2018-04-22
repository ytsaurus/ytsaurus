#include "lazy_yson_consumer.h"

namespace NYT {
namespace NPython {

using namespace NYTree;

using NYson::NDetail::KeyValueSeparatorSymbol;

////////////////////////////////////////////////////////////////////////////////

TLazyYsonConsumer::TLazyYsonConsumer(
    TCallback<TSharedRef()> extractPrefixCallback_,
    TPythonStringCache* keyCacher,
    const TNullable<TString>& encoding,
    bool alwaysCreateAttributes)
    : ExtractPrefixCallback_(extractPrefixCallback_)
    , KeyCacher_(keyCacher)
    , LazyDictConsumer_(new TLazyDictProducer(encoding, alwaysCreateAttributes))
{ }

void TLazyYsonConsumer::OnListItem()
{
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnListItem();
    }
}

void TLazyYsonConsumer::OnKeyedItem(const TStringBuf& key)
{
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnKeyedItem(key);
        return;
    }

    if (Balance_ == 1) {
        ItemKey_ = KeyCacher_->GetPythonString(key);
        ExtractPrefixCallback_.Run();
    }
}

void TLazyYsonConsumer::OnBeginAttributes()
{
    Balance_++;
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnBeginAttributes();
        return;
    }

    if (Balance_ == 1) {
        LazyDictConsumer_->OnBeginAttributes();
    }
}

void TLazyYsonConsumer::OnEndAttributes()
{
    Balance_--;
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnEndAttributes();
        return;
    }

    if (Balance_ == 0) {
        LazyDictConsumer_->OnEndAttributes();
    }
}

void TLazyYsonConsumer::OnRaw(const TStringBuf& /*yson*/, NYson::EYsonType /*type*/)
{
    Y_UNREACHABLE();
}

void TLazyYsonConsumer::OnStringScalar(const TStringBuf& value)
{
    OnItem();
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnStringScalar(value);
    }

    OnItemConsumed();
}

void TLazyYsonConsumer::OnInt64Scalar(i64 value)
{
    OnItem();
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnInt64Scalar(value);
    }

    OnItemConsumed();
}

void TLazyYsonConsumer::OnUint64Scalar(ui64 value)
{
    OnItem();
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnUint64Scalar(value);
    }

    OnItemConsumed();
}

void TLazyYsonConsumer::OnDoubleScalar(double value)
{
    OnItem();
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnDoubleScalar(value);
    }

    OnItemConsumed();
}

void TLazyYsonConsumer::OnBooleanScalar(bool value)
{
    OnItem();
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnBooleanScalar(value);
    }

    OnItemConsumed();
}

void TLazyYsonConsumer::OnEntity()
{
    OnItem();
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnEntity();
    }

    OnItemConsumed();
}

void TLazyYsonConsumer::OnBeginList()
{
    OnItem();
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnBeginList();
    }

    Balance_++;
}

void TLazyYsonConsumer::OnEndList()
{
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnEndList();
    }

    Balance_--;
    OnItemConsumed();
}

void TLazyYsonConsumer::OnBeginMap()
{
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnBeginMap();
    }

    Balance_++;
}

void TLazyYsonConsumer::OnEndMap()
{
    if (!IsLazyDictObject_) {
        LazyDictConsumer_->GetPythonObjectBuilder()->OnEndMap();
    }

    Balance_--;
    OnItemConsumed();
}

bool TLazyYsonConsumer::HasObject() const
{
    return !Objects_.empty();
}

PyObject* TLazyYsonConsumer::ExtractObject()
{
    auto object = Objects_.front();
    Objects_.pop();
    return object;
}

void TLazyYsonConsumer::OnItemConsumed()
{
    if (Balance_ == 1 && IsLazyDictObject_) {
        auto value = ExtractPrefixCallback_.Run();

        // The prefix contains KeyValueSeparatorSymbol and an arbitrary amount of spaces before it
        int separatorPos = -1;
        for (int index = 0; index < value.Size(); ++index) {
            if (value[index] == KeyValueSeparatorSymbol) {
                separatorPos = index;
                break;
            }
        }
        YCHECK(separatorPos != -1);

        value = value.Slice(separatorPos + 1, value.Size());
        LazyDictConsumer_->OnKeyValue(Py::Object(ItemKey_), value);
    } else if (Balance_ == 0) {
        if (!IsLazyDictObject_) {
            LazyDictConsumer_->SetObject();
        }
        Objects_.push(LazyDictConsumer_->ExtractObject().ptr());
    }
}

void TLazyYsonConsumer::OnItem()
{
    if (Balance_ == 0) {
        IsLazyDictObject_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
