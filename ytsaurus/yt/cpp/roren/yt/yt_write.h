#pragma once

#include "yt_io_private.h"

#include <yt/cpp/roren/interface/transforms.h>

#include <yt/cpp/mapreduce/interface/common.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TWriteTransform<T> YtWrite(const NYT::TRichYPath& path, const NYT::TTableSchema& schema)
{
    NPrivate::IRawYtWritePtr inner;
    if constexpr (std::is_same_v<T, NYT::TNode>) {
        inner = NPrivate::MakeYtNodeWrite(path, schema);
    } else {
        static_assert(std::is_same_v<T, NYT::TNode>, "unknown YT reader");
    }
    return TWriteTransform<T>{inner};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
