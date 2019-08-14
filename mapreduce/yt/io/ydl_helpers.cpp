#include "ydl_helpers.h"

#include <mapreduce/yt/interface/io.h>

#include <util/stream/str.h>
#include <util/stream/file.h>
#include <util/folder/path.h>
#include <util/string/cast.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

TVector<ui64> GetJobTypeHashes(const TString& fileName)
{
    Y_ENSURE_EX(TFsPath(fileName).Exists(),
        TIOException() << "Cannot load '" << fileName << "' file");

    TVector<ui64> hashes;
    TIFStream input(fileName);
    TString line;
    while (input.ReadLine(line)) {
        hashes.push_back(FromString<ui64>(line));
    }

    return hashes;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> GetJobInputTypeHashes()
{
    return GetJobTypeHashes("ydl_input");
}

TVector<ui64> GetJobOutputTypeHashes()
{
    return GetJobTypeHashes("ydl_output");
}

void ValidateYdlTypeHash(
    ui64 hash,
    size_t tableIndex,
    const TVector<ui64>& typeHashes,
    bool isRead)
{
    const char* direction = isRead ? "input" : "output";

    Y_ENSURE_EX(tableIndex < typeHashes.size(),
        TIOException() <<
            "Table index " << tableIndex <<
            " is out of range [0, " << typeHashes.size() <<
            ") in " << direction);

    Y_ENSURE_EX(hash == typeHashes[tableIndex],
        TIOException() <<
        "Invalid row type at index " << tableIndex <<
        " in " << direction);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
