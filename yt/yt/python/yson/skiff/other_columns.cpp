#include "other_columns.h"

#include <yt/yt/python/common/helpers.h>

#include <yt/yt/python/yson/serialize.h>
#include <yt/yt/python/yson/pull_object_builder.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/string/builder.h>

#include <CXX/Extensions.hxx> // pycxx
#include <CXX/Objects.hxx> // pycxx

namespace NYT::NPython {

using namespace NSkiff;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TSkiffOtherColumns::TSkiffOtherColumns(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
    : PythonClass(self, args, kwargs)
{
    if (args.size() > 1) {
        throw Py::TypeError("TSkiffOtherColumns.__init__ takes exactly 1 argument");
    }
    if (args.size() == 0) {
        // Object is in uninitialized state.
        return;
    }
    auto arg = args.getItem(0);
    if (PyBytes_Check(arg.ptr())) {
        UnparsedBytesObj_ = Py::Bytes(arg);
    } else if (arg.isMapping()) {
        Map_ = arg;
    } else {
        throw Py::TypeError("TSkiffOtherColumns.__init__ argument must have type \"bytes\" or be a mapping");
    }
}

int TSkiffOtherColumns::mapping_length()
{
    MaybeMaterializeMap();
    return Map_->length();
}

Py::Object TSkiffOtherColumns::mapping_subscript(const Py::Object& key)
{
    MaybeMaterializeMap();
    return Map_->getItem(key);
}

int TSkiffOtherColumns::mapping_ass_subscript(const Py::Object& key, const Py::Object& value)
{
    MaybeMaterializeMap();
    Map_->setItem(key, value);
    return 0;
}

int TSkiffOtherColumns::sequence_contains(const Py::Object& key)
{
    MaybeMaterializeMap();
    return Map_->hasKey(key);
}

Py::Object TSkiffOtherColumns::repr()
{
    MaybeMaterializeMap();
    return Map_->str();
}

Py::Object TSkiffOtherColumns::DeepCopy(const Py::Tuple& /*args*/)
{
    Py::Callable classType(TSkiffOtherColumns::type());
    return classType.apply(Py::TupleN(Py::ConvertToPythonString(GetYsonString().AsStringBuf())), Py::Dict());
}

TStringBuf TSkiffOtherColumns::GetUnparsedBytes() const
{
    char* buffer;
    Py_ssize_t size;
    Y_ABORT_UNLESS(UnparsedBytesObj_);
    if (PyBytes_AsStringAndSize(UnparsedBytesObj_->ptr(), &buffer, &size) == -1) {
        throw Py::Exception();
    }
    return TStringBuf(buffer, size);
}

TYsonStringBuf TSkiffOtherColumns::GetYsonString()
{
    if (UnparsedBytesObj_) {
        return TYsonStringBuf(GetUnparsedBytes(), EYsonType::Node);
    }
    if (!Map_) {
        throw Py::RuntimeError("TSkiffOtherColumns is unitilialized, GetYsonString should not be called");
    }
    CachedYsonString_ = ConvertToYsonString(Py::Object(*Map_));
    return CachedYsonString_;
}

void TSkiffOtherColumns::InitType()
{
    behaviors().name("yt_yson_bindings.yson_lib.SkiffOtherColumns");
    behaviors().doc("Very lazy dict of skiff $other_columns");
    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportMappingType(
        behaviors().support_mapping_ass_subscript |
        behaviors().support_mapping_subscript |
        behaviors().support_mapping_length);
    behaviors().supportSequenceType(
        behaviors().support_sequence_contains);
    behaviors().supportRepr();
    behaviors().supportCompare();

    PYCXX_ADD_VARARGS_METHOD(__deepcopy__, DeepCopy, "Deepcopy");

    behaviors().readyType();
}

void TSkiffOtherColumns::MaybeMaterializeMap()
{
    if (Map_) {
        return;
    }
    if (!UnparsedBytesObj_) {
        Map_ = Py::Dict();
        return;
    }

    TMemoryInput input(GetUnparsedBytes());
    try {
        TYsonPullParser parser(&input, EYsonType::Node);
        TPullObjectBuilder builder(&parser, /* alwaysCreateAttributes */ false, DefaultEncoding);
        Map_ = Py::Object(builder.ParseObjectLazy().release(), /* owned */ true);
    } catch (const std::exception& exception) {
        throw Py::RuntimeError(::TStringBuilder() << "Failed to parse lazy yson: " << exception.what());
    }
    UnparsedBytesObj_.reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
