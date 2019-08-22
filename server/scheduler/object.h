#pragma once

#include "private.h"

#include <yt/core/yson/string.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TObject
{
public:
    TObject(
        TObjectId id,
        NYT::NYson::TYsonString labels);
    virtual ~TObject() = default;

    const TObjectId& GetId() const;
    const NYT::NYson::TYsonString& GetLabels() const;

    template <class T>
    const T* As() const;
    template <class T>
    T* As();

private:
    const TObjectId Id_;
    const NYT::NYson::TYsonString Labels_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_
