#pragma once

#include "private.h"

#include <yt/core/yson/string.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TObject
{
public:
    TObject(
        const TObjectId& id,
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

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_
