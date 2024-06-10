#include "destination_writer.h"

#include <bigrt/lib/writer/swift/factory.h>
#include <bigrt/lib/writer/yt_queue/factory.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IDestinationWriterFactory);
DEFINE_REFCOUNTED_TYPE(IDestinationWriter);

enum class EDestinationType
{
    Swift,
    Qyt,
};

////////////////////////////////////////////////////////////////////////////////

class TDestinationWriter : public IDestinationWriter
{
public:
    TDestinationWriter(NBigRT::IWriterPtr writer, EDestinationType destinationType)
        : Writer_(std::move(writer))
        , DestinationType_(destinationType)
    {
    }

    NBigRT::IWriterPtr Clone() override
    {
        return NYT::New<TDestinationWriter>(Writer_, DestinationType_);
    }

    NYT::TFuture<TTransactionWriter> ExtractAsyncWriter() override
    {
        return Writer_->ExtractAsyncWriter();
    }

    void Write(const TSerializedRow& row) override
    {
        switch (DestinationType_) {
            case EDestinationType::Swift: {
                auto& writer = dynamic_cast<NBigRT::ISwiftQueueWriter&>(*Writer_);
                writer.Write(
                    row.DataSource,
                    NYT::New<NBigRT::NSwiftQueue::TWriteRow>(NBigRT::NSwiftQueue::TWriteRow{
                        .DestinationShard = row.Shard,
                        .SeqNo = row.SeqNo,
                        .Data = row.Data,
                    })
                );
                break;
            }
            case EDestinationType::Qyt: {
                auto& writer = dynamic_cast<NBigRT::IYtQueueWriter&>(*Writer_);
                writer.Write(row.Shard, row.Data);
                break;
            }
            default: {
                Y_ABORT();
            }
        }
    }

    ui64 GetShardsCount() override {
        switch (DestinationType_) {
            case EDestinationType::Swift: {
                auto& writer = dynamic_cast<NBigRT::ISwiftQueueWriter&>(*Writer_);
                return writer.GetShardsCount();
            }
            case EDestinationType::Qyt: {
                auto& writer = dynamic_cast<NBigRT::IYtQueueWriter&>(*Writer_);
                return writer.GetShardsCount();
            }
            default: {
                Y_ABORT();
            }
        }
    }

private:
    NBigRT::IWriterPtr Writer_;
    EDestinationType DestinationType_;
};

////////////////////////////////////////////////////////////////////////////////

class TDestinationWriterFactory : public IDestinationWriterFactory
{
public:
    TDestinationWriterFactory(NBigRT::IYtQueueWriterFactoryPtr factory)
        : TDestinationWriterFactory(std::move(factory), EDestinationType::Qyt)
    {
    }

    TDestinationWriterFactory(NBigRT::ISwiftQueueWriterFactoryPtr factory)
        : TDestinationWriterFactory(std::move(factory), EDestinationType::Swift)
    {
    }

    NBigRT::IWriterPtr Make(
        ui64 shard, NSFStats::TSolomonContext solomonContext
    ) override
    {
        return NYT::New<TDestinationWriter>(Factory_->Make(shard, solomonContext), DestinationType_);
    }

private:
    TDestinationWriterFactory(NBigRT::IWriterFactoryPtr factory, EDestinationType destinationType)
        : Factory_(std::move(factory))
        , DestinationType_(destinationType)
    {
    }

    NBigRT::IWriterFactoryPtr Factory_;
    EDestinationType DestinationType_;
};

IDestinationWriterFactoryPtr CreateDestinationWriterFactory(
    NBigRT::IYtQueueWriterFactoryPtr factory
) {
    return NYT::New<TDestinationWriterFactory>(std::move(factory));
}

IDestinationWriterFactoryPtr CreateDestinationWriterFactory(
    NBigRT::ISwiftQueueWriterFactoryPtr factory
) {
    return NYT::New<TDestinationWriterFactory>(std::move(factory));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
