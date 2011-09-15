#include "fs.h"

#include "../logging/log.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>


// for GetAvaibaleSpace()
#if defined(_linux_)
#include <sys/vfs.h>
#elif defined(_freebsd_) || defined(_darwin_)
#include <sys/param.h>
#include <sys/mount.h>
#elif defined (_win_)
#include <windows.h>
#endif

namespace NYT {
namespace NFS {

//////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("FS");

//////////////////////////////////////////////////////////////////////////////

bool Remove(Stroka name)
{
#if defined(_win_)
    return DeleteFile(~name);
#else
    struct stat sb;

    if (int result = lstat(~name, &sb))
        return result == 0;

    if (!S_ISDIR(sb.st_mode))
        return ::remove(~name) == 0;

    return ::rmdir(~name) == 0;
#endif
}

bool Rename(Stroka oldName, Stroka newName)
{
#if defined(_win_)
    return MoveFileEx(~oldName, ~newName, MOVEFILE_REPLACE_EXISTING) != 0;
#else
    return ::rename(~oldName, ~newName) == 0;
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

i64 GetAvailableSpace(const Stroka& path) throw(yexception)
{
#if !defined( _win_)
    struct statfs fsData;
    int res = statfs(~path, &fsData);
    if (res != 0) {
        throw yexception() << Sprintf("statfs failed on location %s", ~path);
    }
    return fsData.f_bavail * fsData.f_bsize;
#else
    ui64 freeBytes;
    int res = GetDiskFreeSpaceExA(
        ~path,
        (PULARGE_INTEGER)&freeBytes,
        (PULARGE_INTEGER)NULL,
        (PULARGE_INTEGER)NULL);
    if (res == 0) {
        throw yexception() << Sprintf("GetDiskFreeSpaceExA failed on location %s", ~path);
    }
    return freeBytes;
#endif
}

void MakePathIfNotExist(Stroka path, int mode)
{
    MakePathIfNotExist(~path, mode);
}

i64 GetFileSize(const Stroka& filePath) {
#ifdef _win_
    WIN32_FIND_DATA fData;
    HANDLE h = FindFirstFileA(~name, &fData);
    if (h == INVALID_HANDLE_VALUE)
        return -1;
    FindClose(h);
    return (((i64)fData.nFileSizeHigh) * (i64(MAXDWORD)+1)) + (i64)fData.nFileSizeLow;
#elif defined(_unix_)
    struct stat buf;
    int r = stat(~filePath, &buf);
    if (r == -1)
        return -1;
    return (i64)buf.st_size;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS
} // namespace NYT
