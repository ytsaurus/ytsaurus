#include <yt/yt/flow/library/cpp/serializer/state.h>

#include <yt/yt/core/ytree/convert.h>

#include <contrib/python/py3c/py3c.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRef BorrowBytes(PyObject* value)
{
    // The argument tuple keeps these buffers alive until the result is copied.
    return value == Py_None
        ? TSharedRef::MakeEmpty()
        : TSharedRef(PyBytes_AsString(value), PyBytes_Size(value), nullptr);
}

PyObject* UnpackStateBytes(PyObject* /*self*/, PyObject* args)
{
    PyObject* compressed;
    PyObject* patch;
    PyObject* format;
    if (!PyArg_ParseTuple(args, "OOO:unpack_state_bytes", &compressed, &patch, &format)) {
        return nullptr;
    }
    for (auto* value : {compressed, patch, format}) {
        if (value != Py_None && !PyBytes_Check(value)) {
            PyErr_SetString(PyExc_TypeError, "Packed columns and format must be bytes or None");
            return nullptr;
        }
    }
    try {
        auto stateFormat = format == Py_None
            ? nullptr
            : NYTree::ConvertTo<NYsonSerializer::TFormatPtr>(NYson::TYsonStringBuf(BorrowBytes(format).ToStringBuf()));
        auto unpacked = NYsonSerializer::UnpackState(BorrowBytes(compressed), BorrowBytes(patch), stateFormat);
        return PyBytes_FromStringAndSize(unpacked.Begin(), unpacked.Size());
    } catch (const std::bad_alloc&) {
        return PyErr_NoMemory();
    } catch (const std::exception& error) {
        PyErr_SetString(PyExc_ValueError, error.what());
        return nullptr;
    }
}

PyMethodDef Methods[] = {
    {"unpack_state_bytes", UnpackStateBytes, METH_VARARGS, "Restore packed Flow state bytes."},
    {nullptr, nullptr, 0, nullptr},
};

PyModuleDef Module = {
    PyModuleDef_HEAD_INIT,
    "bindings",
    nullptr,
    -1,
    Methods,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow

MODULE_INIT_FUNC(bindings)
{
    return PyModule_Create(&NYT::NFlow::Module);
}
