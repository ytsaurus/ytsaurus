#pragma once

#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

namespace NYql {

void LoadBindings(THashMap<TString, NSQLTranslation::TTableBindingSettings>& dst, TStringBuf jsonText);

} /* namespace NYql */

