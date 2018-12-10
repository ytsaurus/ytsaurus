#pragma once

#include <yt/python/common/helpers.h>

#include <yt/core/yson/lexer_detail.h>

#include <Python.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TLazyListFragmentParser
{
public:
    TLazyListFragmentParser();

    TLazyListFragmentParser(
        IInputStream* stream,
        const std::optional<TString>& encoding,
        bool alwaysCreateAttributes,
        TPythonStringCache* keyCacher);

    ~TLazyListFragmentParser();

    PyObject* NextItem();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
