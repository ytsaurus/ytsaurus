#include "control.h"
#include <contrib/ydb/core/tx/columnshard/data_sharing/source/session/source.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

TEvStartToSource::TEvStartToSource(const TSourceSession& session) {
    *Record.MutableSession() = session.SerializeDataToProto();
}

}