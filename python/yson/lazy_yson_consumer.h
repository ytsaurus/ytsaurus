#pragma once

#include "lazy_dict_producer.h"

#include <yt/python/common/helpers.h>
#include <yt/python/common/stream.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/lexer_detail.h>
#include <yt/core/yson/parser.h>

#include <Python.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TLazyYsonConsumer
    : public NYson::IYsonConsumer
{
public:
    TLazyYsonConsumer(
        TCallback<TSharedRef()> extractPrefixCallback_,
        TPythonStringCache* keyCacher,
        const TNullable<TString>& encoding,
        bool alwaysCreateAttributes);

    void OnListItem();
    void OnKeyedItem(const TStringBuf& key);
    void OnBeginAttributes();
    void OnEndAttributes();
    void OnRaw(const TStringBuf& /*yson*/, NYson::EYsonType /*type*/);
    void OnStringScalar(const TStringBuf& value);
    void OnInt64Scalar(i64 value);
    void OnUint64Scalar(ui64 value);
    void OnDoubleScalar(double value);
    void OnBooleanScalar(bool value);
    void OnEntity();
    void OnBeginList();
    void OnEndList();
    void OnBeginMap();
    void OnEndMap();

    bool HasObject() const;

    PyObject* ExtractObject();

private:
    void OnItemConsumed();
    void OnItem();

    int Balance_ = 0;

    std::queue<PyObject*> Objects_;

    TCallback<TSharedRef()> ExtractPrefixCallback_;

    TPythonStringCache* KeyCacher_;
    PyObject* ItemKey_;

    std::unique_ptr<TLazyDictProducer> LazyDictConsumer_;

    bool IsLazyDictObject_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
