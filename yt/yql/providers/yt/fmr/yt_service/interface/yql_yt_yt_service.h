#pragma once

#include <util/system/tempfile.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>

namespace NYql::NFmr {

class IYtService: public TThrRefBase {
public:
    virtual ~IYtService() = default;

    using TPtr = TIntrusivePtr<IYtService>;

    virtual std::variant<THolder<TTempFileHandle>, TError> Download(const TYtTableRef& ytTable) = 0;

    virtual TMaybe<TError> Upload(const TYtTableRef& ytTable, IInputStream& tableContent) = 0;
};

} // namespace NYql::NFmr
