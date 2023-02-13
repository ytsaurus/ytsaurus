#pragma once

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

//! Given parts of YSON and their destinations, merges several YSON strings into one.
//! Faster alternative to casting everything to YSON nodes, merging them and casting
//! this merged YSON back to YSON string.
//! YSON strings `ysonStringBufs` can be of any format, but type must be EYsonType::Node.
//! In a case when one path is prefix of another,
//! only YSON string corresponding to the shortest path will be used.
//! Setting expandLists to true enables expanding lists on '*' symbol in path.
//! There should be a list level for each '*' in path.
TYsonString MergeYsonStrings(
    std::vector<NYPath::TYPath> paths,
    std::vector<TYsonStringBuf> ysonStringBufs,
    EYsonFormat format = EYsonFormat::Binary,
    bool expandLists = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
