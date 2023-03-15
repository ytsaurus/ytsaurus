#pragma once

#include <util/ysaveload.h>

#include <yt/yt/library/profiling/tag.h>

template <>
class TSerializer<NYT::NProfiling::TTagSet> {
public:
    static void Save(IOutputStream* out, const NYT::NProfiling::TTagSet& value);
    static void Load(IInputStream* in, NYT::NProfiling::TTagSet& value);
};


