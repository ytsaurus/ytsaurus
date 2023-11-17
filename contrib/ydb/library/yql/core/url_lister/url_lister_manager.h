#pragma once

#include <contrib/ydb/library/yql/core/url_lister/interface/url_lister.h>
#include <contrib/ydb/library/yql/core/url_lister/interface/url_lister_manager.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>


namespace NYql {

IUrlListerManagerPtr MakeUrlListerManager(
    TVector<IUrlListerPtr> urlListers
);

}
