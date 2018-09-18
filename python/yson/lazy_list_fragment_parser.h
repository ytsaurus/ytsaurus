#pragma once

#include <yt/python/common/helpers.h>

#include <yt/core/yson/lexer_detail.h>

#include <Python.h>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

class TLazyListFragmentParser
{
public:
    TLazyListFragmentParser();

    TLazyListFragmentParser(
        IInputStream* stream,
        const TNullable<TString>& encoding,
        bool alwaysCreateAttributes,
        TPythonStringCache* keyCacher);

    ~TLazyListFragmentParser();

    PyObject* NextItem();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT
