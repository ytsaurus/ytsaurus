#pragma once

#include "common.h"

#include "guid.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TGuid GuidFromProtoGuid(const Stroka& protoGuid);
Stroka ProtoGuidFromGuid(const TGuid& guid);

////////////////////////////////////////////////////////////////////////////////

template<class T>
bool Read(TInputStream& input, T* data)
{
    return input.Load(data, sizeof(T)) == sizeof(T);
}

template<class T>
bool Read(TFile& file, T* data)
{
    return file.Read(data, sizeof(T)) == sizeof(T);
}

template<class T>
void Write(TOutputStream& output, const T& data)
{
    output.Write(&data, sizeof(T));
}

template<class T>
void Write(TFile& file, const T& data)
{
    file.Write(&data, sizeof(T));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

