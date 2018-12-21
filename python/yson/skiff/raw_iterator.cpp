#include "raw_iterator.h"
#include "parser_helpers.h"

#include <yt/core/ytree/convert.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

TSkiffRawIterator::TSkiffRawIterator(Py::PythonClassInstance* self, Py::Tuple& args, Py::Dict& kwargs)
    : Py::PythonClass<TSkiffRawIterator>::PythonClass(self, args, kwargs)
{ }

void TSkiffRawIterator::Initialize(
    IInputStream* inputStream,
    std::unique_ptr<IInputStream> inputStreamHolder,
    const std::vector<Py::PythonClassObject<TSkiffSchemaPython>>& pythonSkiffschemaList,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
{
    YCHECK(inputStreamHolder.get() == inputStream);
    InputStream_ = TStreamReader(inputStream);
    InputStreamHolder_ = std::move(inputStreamHolder);

    Consumer_ = std::make_unique<TPythonSkiffRawRecordBuilder>(
        pythonSkiffschemaList.size(),
        BIND([&] () { ExtractRow(); }));
    Parser_ = CreateSkiffMultiTableParser<TPythonSkiffRawRecordBuilder>(
        Consumer_.get(),
        pythonSkiffschemaList,
        rangeIndexColumnName,
        rowIndexColumnName);
}

void TSkiffRawIterator::ExtractRow()
{
    ui64 readBytesCount = Parser_->GetReadBytesCount();
    auto row = InputStream_.ExtractPrefix(readBytesCount - ExtractedPrefixLength);
    ExtractedPrefixLength = readBytesCount;
    Rows_.push(row);
}

Py::Object TSkiffRawIterator::iter()
{
    return self();
}

PyObject* TSkiffRawIterator::iternext()
{
    while (Rows_.empty() && !IsStreamFinished_) {
        auto length = InputStream_.End() - InputStream_.Current();
        if (length == 0 && !InputStream_.IsFinished()) {
            InputStream_.RefreshBlock();
            continue;
        }

        if (length != 0) {
            Parser_->Read(TStringBuf(InputStream_.Current(), InputStream_.End()));
            InputStream_.Advance(length);
        } else {
            IsStreamFinished_ = true;
            Parser_->Finish();
        }
    }

    if (Rows_.empty()) {
        PyErr_SetNone(PyExc_StopIteration);
        return nullptr;
    }

    auto row = Rows_.front();
    Rows_.pop();
    auto object = Py::Bytes(row.Begin(), row.Size());
    object.increment_reference_count();
    return object.ptr();
}

void TSkiffRawIterator::InitType()
{
    behaviors().name("Skiff raw iterator");
    behaviors().doc("Iterates over stream with skiff rows");
    behaviors().supportGetattro();
    behaviors().supportSetattro();
    behaviors().supportIter();

    behaviors().readyType();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
