#include "data_sink.h"

#include <yt/yt/ytlib/chunk_client/proto/data_sink.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

void FromProto(TDataSink* dataSink, const NProto::TDataSink& protoDataSink)
{
    using NYT::FromProto;

    if (protoDataSink.has_object_path()) {
        dataSink->SetPath(protoDataSink.object_path());
    }

    if (protoDataSink.has_object_id()) {
        dataSink->SetObjectId(FromProto<NObjectClient::TObjectId>(protoDataSink.object_id()));
    }

    if (protoDataSink.has_account()) {
        dataSink->SetAccount(protoDataSink.account());
    }
}

void ToProto(NProto::TDataSink* protoDataSink, const TDataSink& dataSink)
{
    using NYT::ToProto;

    if (dataSink.GetPath()) {
        protoDataSink->set_object_path(*dataSink.GetPath());
    }

    if (dataSink.GetObjectId()) {
        ToProto(protoDataSink->mutable_object_id(), dataSink.GetObjectId());
    }

    if (dataSink.GetAccount()) {
        protoDataSink->set_account(*dataSink.GetAccount());
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDataSinkDirectoryExt* protoDataSinkDirectory,
    const TDataSinkDirectoryPtr& dataSinkDirectory)
{
    using NYT::ToProto;

    for (const auto& dataSink : dataSinkDirectory->DataSinks()) {
        auto* protoDataSink = protoDataSinkDirectory->add_data_sinks();
        ToProto(protoDataSink, dataSink);
    }
}

void FromProto(
    TDataSinkDirectoryPtr* dataSinkDirectory,
    const NProto::TDataSinkDirectoryExt& protoDataSinkDirectory)
{
    using NYT::FromProto;

    *dataSinkDirectory = New<TDataSinkDirectory>();
    auto& dataSinks = (*dataSinkDirectory)->DataSinks();
    dataSinks.reserve(protoDataSinkDirectory.data_sinks_size());
    for (const auto& protoDataSink : protoDataSinkDirectory.data_sinks()) {
        dataSinks.emplace_back(FromProto<TDataSink>(protoDataSink));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
