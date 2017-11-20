#include <mapreduce/yt/interface/io.h>

#include "node_table_reader.h"
#include "proto_table_reader.h"
#include "yamr_table_reader.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TInputStreamProxy
    : public TProxyInput
{
public:
    TInputStreamProxy(IInputStream* stream)
        : Stream_(stream)
    { }

protected:
    size_t DoRead(void* buf, size_t len)
    {
        return Stream_->Read(buf, len);
    }

private:
    IInputStream* Stream_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <>
TTableReaderPtr<TNode> CreateTableReader<TNode>(
    IInputStream* stream, const TTableReaderOptions& options)
{
    auto impl = ::MakeIntrusive<TNodeTableReader>(::MakeIntrusive<TInputStreamProxy>(stream), options.SizeLimit_);
    return new TTableReader<TNode>(impl);
}

template <>
TTableReaderPtr<TYaMRRow> CreateTableReader<TYaMRRow>(
    IInputStream* stream, const TTableReaderOptions& /*options*/)
{
    auto impl = ::MakeIntrusive<TYaMRTableReader>(::MakeIntrusive<TInputStreamProxy>(stream));
    return new TTableReader<TYaMRRow>(impl);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
    IInputStream* stream,
    const TTableReaderOptions& /* options */,
    const ::google::protobuf::Descriptor* descriptor)
{
    return new TLenvalProtoTableReader(
        ::MakeIntrusive<TInputStreamProxy>(stream),
        {descriptor});
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
