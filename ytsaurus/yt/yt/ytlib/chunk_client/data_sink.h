#pragma once

#include "public.h"

#include <optional>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TDataSink
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::optional<NYPath::TYPath>, Path);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TObjectId, ObjectId);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, Account);

    TDataSink() = default;
};

void FromProto(TDataSink* dataSink, const NProto::TDataSink& protoDataSink);
void ToProto(NProto::TDataSink* protoDataSink, const TDataSink& dataSink);

////////////////////////////////////////////////////////////////////////////////

class TDataSinkDirectory
    : public TRefCounted
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TDataSink>, DataSinks);
};

DEFINE_REFCOUNTED_TYPE(TDataSinkDirectory)

void FromProto(
    TDataSinkDirectoryPtr* dataSinkDirectory,
    const NProto::TDataSinkDirectoryExt& protoDataSinkDirectory);

void ToProto(
    NProto::TDataSinkDirectoryExt* protoDataSinkDirectory,
    const TDataSinkDirectoryPtr& dataSinkDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
