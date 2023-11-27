#pragma once

#include "yt_io_private.h"
#include "yt_proto_io.h"

#include <yt/cpp/roren/interface/transforms.h>
#include <yt/cpp/roren/interface/private/serializable.h>
#include <yt/cpp/roren/interface/private/raw_transform.h>
#include <yt/cpp/roren/interface/private/row_vtable.h>

#include <yt/cpp/mapreduce/interface/common.h>

#include <util/stream/mem.h>
#include <util/ysaveload.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TReadTransform<T> YtRead(NYT::TRichYPath path)
{
    NPrivate::IRawYtReadPtr inner;
    if constexpr (std::is_same_v<T, NYT::TNode>) {
        inner = NPrivate::MakeYtNodeInput(std::move(path));
    } else if constexpr (std::is_base_of_v<::google::protobuf::Message, T>) {
        inner = NPrivate::MakeYtProtoRead<T>(std::move(path));
    } else {
        static_assert(TDependentFalse<T>, "unknown YT reader");
    }
    return TReadTransform<T>{inner};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
