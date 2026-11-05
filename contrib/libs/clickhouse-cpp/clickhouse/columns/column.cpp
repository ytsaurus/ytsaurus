#include "column.h"

namespace clickhouse {

bool Column::LoadPrefix(InputStream*, size_t) {
    /// does nothing by default
    return true;
}

bool Column::Load(InputStream* input, size_t rows) {
    return LoadPrefix(input, rows) && LoadBody(input, rows);
}

void Column::SavePrefix(OutputStream*) {
    /// does nothing by default
}

/// Saves column data to output stream.
void Column::Save(OutputStream* output) {
    SavePrefix(output);
    SaveBody(output);
}

}
