#include "fs.h"

#include "../logging/log.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>

namespace NYT {
namespace NFS {

//////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("FS");

//////////////////////////////////////////////////////////////////////////////

bool Remove(const char* name)
{
#if defined(_win_)
    return DeleteFile(name);
#else
    struct stat sb;

    if (int result = lstat(name, &sb))
        return result == 0;

    if (!S_ISDIR(sb.st_mode))
        return ::remove(name) == 0;

    return ::rmdir(name) == 0;
#endif
}

bool Rename(const char* oldName, const char* newName)
{
#if defined(_win_)
    return MoveFileEx(oldName, newName, MOVEFILE_REPLACE_EXISTING) != 0;
#else
    return ::rename(oldName, newName) == 0;
#endif
}


Stroka GetFileName(Stroka filePath)
{
    size_t delimPos = filePath.rfind('/');
#ifdef _win32_
    if (delimPos == Stroka::npos) {
        // There's a possibility of Windows-style path
        delimPos = filePath.rfind('\\');
    }
#endif
    return (delimPos == Stroka::npos) ? filePath : filePath.substr(delimPos+1);
}

Stroka GetFileExtension(Stroka filePath)
{
    i32 dotPosition = filePath.find_last_of('.');
    if (dotPosition < 0) {
        return "";
    }
    return filePath.substr(dotPosition + 1, filePath.size() - dotPosition - 1);
}

Stroka GetFileNameWithoutExtension(Stroka filePath)
{
    filePath = GetFileName(filePath);
    i32 dotPosition = filePath.find_last_of('.');
    if (dotPosition < 0) {
        return filePath;
    }
    return filePath.substr(0, dotPosition);
}

void CleanTempFiles(Stroka location)
{
    LOG_INFO("Cleaning temp files in %s", ~location);
    TFileList fileList;
    fileList.Fill(location);
    const char* fileName;
    while ((fileName = fileList.Next()) != NULL) {
        Stroka fullName = location + "/" + Stroka(fileName);
        if (fullName.has_suffix(TempFileSuffix)) {
            LOG_INFO("Removing file %s", ~fullName);
            if (!NFS::Remove(~fullName)) {
                LOG_ERROR("Error removing %s",  ~fullName);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
