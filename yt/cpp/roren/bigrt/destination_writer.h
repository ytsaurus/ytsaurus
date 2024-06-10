#pragma once

#include <bigrt/lib/writer/base/factory.h>
#include <bigrt/lib/writer/base/writer.h>
#include <bigrt/lib/writer/swift/fwd.h>
#include <bigrt/lib/writer/yt_queue/fwd.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

struct TSerializedRow {
    ui64 Shard;
    i64 SeqNo;
    TString Data;
    TString DataSource;

    Y_SAVELOAD_DEFINE(Shard, SeqNo, Data, DataSource);
};

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(IDestinationWriterFactory);
DECLARE_REFCOUNTED_CLASS(IDestinationWriter);

class IDestinationWriter : public NBigRT::IWriter
{
public:
    virtual void Write(const TSerializedRow& row) = 0;
    virtual ui64 GetShardsCount() = 0;
};

class IDestinationWriterFactory: public NBigRT::IWriterFactory
{
public:
    using TWriter = IDestinationWriter;
};

////////////////////////////////////////////////////////////////////////////////

IDestinationWriterFactoryPtr CreateDestinationWriterFactory(
    NBigRT::IYtQueueWriterFactoryPtr factory
);

IDestinationWriterFactoryPtr CreateDestinationWriterFactory(
    NBigRT::ISwiftQueueWriterFactoryPtr factory
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPrivate

} // namespace NRoren
