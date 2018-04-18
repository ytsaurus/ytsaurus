#include "fs.h"
#include "finally.h"

#include <yt/core/logging/log.h>
#include <yt/core/misc/ref_counted.h>

#include <yt/core/misc/proc.h>

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>
#include <util/system/shellcommand.h>

#include <array>

#if defined(_unix_)
    #include <sys/mount.h>
    #include <sys/stat.h>
    #include <fcntl.h>
#endif

#if defined(_linux_)
    #include <mntent.h>
    #include <sys/vfs.h>
    #include <sys/quota.h>
    #include <sys/types.h>
    #include <sys/sendfile.h>
#elif defined(_freebsd_) || defined(_darwin_)
    #include <sys/param.h>
    #include <sys/mount.h>
#elif defined (_win_)
    #include <comutil.h>
    #include <shlobj.h>
#endif

namespace NYT {
namespace NFS {

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("FS");

////////////////////////////////////////////////////////////////////////////////

namespace {

void ThrowNotSupported()
{
    THROW_ERROR_EXCEPTION("Unsupported platform");
}

} // namespace

bool Exists(const TString& path)
{
#ifdef _win32_
    return GetFileAttributesA(~path) != 0xFFFFFFFF;
#else
    return access(~path, F_OK) == 0;
#endif
}

void Remove(const TString& path)
{
    bool ok;
#ifdef _win_
    ok = DeleteFileA(~path);
#else
    struct stat sb;
    ok = lstat(~path, &sb) == 0;
    if (ok) {
        if (S_ISDIR(sb.st_mode)) {
            ok = rmdir(~path) == 0;
        } else {
            ok = remove(~path) == 0;
        }
    }
#endif
    if (!ok) {
        THROW_ERROR_EXCEPTION("Cannot remove %v",
            path)
            << TError::FromSystem();
    }
}

void Replace(const TString& source, const TString& destination)
{
    if (NFS::Exists(destination)) {
        NFS::Remove(destination);
    }
    NFS::Rename(source, destination);
}

void RemoveRecursive(const TString& path)
{
    RemoveDirWithContents(path);
}

void Rename(const TString& source, const TString& destination)
{
    bool ok;
#if defined(_win_)
    ok = MoveFileEx(~source, ~destination, MOVEFILE_REPLACE_EXISTING) != 0;
#else
    ok = rename(~source, ~destination) == 0;
#endif
    if (!ok) {
        THROW_ERROR_EXCEPTION("Cannot rename %v to %v",
            source,
            destination)
            << TError::FromSystem();
    }
}

TString GetFileName(const TString& path)
{
    size_t slashPosition = path.find_last_of(LOCSLASH_C);
    if (slashPosition == TString::npos) {
        return path;
    }
    return path.substr(slashPosition + 1);
}

TString GetDirectoryName(const TString& path)
{
    auto absPath = CombinePaths(NFs::CurrentWorkingDirectory(), path);
    size_t slashPosition = absPath.find_last_of(LOCSLASH_C);
    return absPath.substr(0, slashPosition);
}

TString GetRealPath(const TString& path)
{
    auto curPath = CombinePaths(NFs::CurrentWorkingDirectory(), path);
    std::vector<TString> parts;
    while (!Exists(curPath)) {
        auto filename = GetFileName(curPath);
        if (filename == ".") {
            // Do nothing.
        } else if (filename == ".." || parts.empty() || parts.back() != "..") {
            parts.push_back(filename);
        } else {
            parts.pop_back();
        }
        curPath = GetDirectoryName(curPath);
        if (curPath.Empty()) {
            break;
        }
    }
    parts.push_back(RealPath(curPath));

#ifdef YT_IN_ARCADIA
    Reverse(parts.begin(), parts.end());
#else
    reverse(parts.begin(), parts.end());
#endif
    return CombinePaths(parts);
}

bool CheckPathIsRelativeAndGoesInside(const TString& path)
{
    std::vector<TString> parts;

    TString currentPath = path;
    while (true) {
        size_t slashPosition = currentPath.find_last_of(LOCSLASH_C);
        if (slashPosition == TString::npos) {
            if (currentPath.empty()) {
                // Path is absolute.
                return false;
            } else {
                parts.push_back(currentPath);
                break;
            }
        } else {
            parts.push_back(currentPath.substr(slashPosition + 1));
            currentPath = currentPath.substr(0, slashPosition);
        }
    }

    int inCount = 0;
    int outCount = 0;
    for (const auto& part : parts) {
        if (part == ".") {
            continue;
        } else if (part == "..") {
            ++outCount;
        } else {
            ++inCount;
        }
    }
    return inCount >= outCount;
}

TString GetFileExtension(const TString& path)
{
    size_t dotPosition = path.find_last_of('.');
    if (dotPosition == TString::npos) {
        return "";
    }
    size_t slashPosition = path.find_last_of(LOCSLASH_C);
    if (slashPosition != TString::npos && dotPosition < slashPosition) {
        return "";
    }
    return path.substr(dotPosition + 1);
}

TString GetFileNameWithoutExtension(const TString& path)
{
    auto fileName = GetFileName(path);
    size_t dotPosition = fileName.find_last_of('.');
    if (dotPosition == TString::npos) {
        return fileName;
    }
    return fileName.substr(0, dotPosition);
}

void CleanTempFiles(const TString& path)
{
    LOG_INFO("Cleaning temp files in %v", path);

    // TODO(ignat): specify suffix in EnumerateFiles.
    auto entries = EnumerateFiles(path, std::numeric_limits<int>::max());
    for (const auto& entry : entries) {
        if (entry.EndsWith(TempFileSuffix)) {
            auto fileName = NFS::CombinePaths(path, entry);
            LOG_INFO("Removing file %v", fileName);
            NFS::Remove(fileName);
        }
    }
}

std::vector<TString> EnumerateFiles(const TString& path, int depth)
{
    std::vector<TString> result;
    if (NFS::Exists(path)) {
        TFileList list;
        list.Fill(path, TStringBuf(), TStringBuf(), depth);
        int size = list.Size();
        for (int i = 0; i < size; ++i) {
            result.push_back(list.Next());
        }
    }
    return result;
}

std::vector<TString> EnumerateDirectories(const TString& path, int depth)
{
    std::vector<TString> result;
    if (NFS::Exists(path)) {
        TDirsList list;
        list.Fill(path, TStringBuf(), TStringBuf(), depth);
        int size = list.Size();
        for (int i = 0; i < size; ++i) {
            result.push_back(list.Next());
        }
    }
    return result;
}

TDiskSpaceStatistics GetDiskSpaceStatistics(const TString& path)
{
    TDiskSpaceStatistics result;
    bool ok;
#ifdef _win_
    ok = GetDiskFreeSpaceEx(
        ~path,
        (PULARGE_INTEGER) &result.AvailableSpace,
        (PULARGE_INTEGER) &result.TotalSpace,
        (PULARGE_INTEGER) &result.FreeSpace) != 0;
#else
    struct statfs fsData;
    ok = statfs(~path, &fsData) == 0;
    result.TotalSpace = (i64) fsData.f_blocks * fsData.f_bsize;
    result.AvailableSpace = (i64) fsData.f_bavail * fsData.f_bsize;
    result.FreeSpace = (i64) fsData.f_bfree * fsData.f_bsize;
#endif

    if (!ok) {
        THROW_ERROR_EXCEPTION("Failed to get disk space statistics for %v",
            path)
            << TError::FromSystem();
    }

    return result;
}

void MakeDirRecursive(const TString& path, int mode)
{
    MakePathIfNotExist(~path, mode);
}

TFileStatistics GetFileStatistics(const TString& path)
{
    TFileStatistics statistics;
#ifdef _unix_
    struct stat fileStat;
    int result = ::stat(~path, &fileStat);
#else
    WIN32_FIND_DATA findData;
    HANDLE handle = ::FindFirstFileA(~path, &findData);
#endif

#ifdef _unix_
    if (result == -1) {
#else
    if (handle == INVALID_HANDLE_VALUE) {
#endif
        THROW_ERROR_EXCEPTION("Failed to get statistics for %v",
            path)
            << TError::FromSystem();
    }

#ifdef _unix_
    statistics.Size = static_cast<i64>(fileStat.st_size);
    statistics.ModificationTime = TInstant::Seconds(fileStat.st_mtime);
    statistics.AccessTime = TInstant::Seconds(fileStat.st_atime);
#else
    ::FindClose(handle);
    statistics.Size = (static_cast<i64>(findData.nFileSizeHigh) << 32) + static_cast<i64>(findData.nFileSizeLow);
    statistics.ModificationTime = TInstant::MicroSeconds(ToMicroSeconds(findData.ftLastWriteTime));
    statistics.AccessTime = TInstant::MicroSeconds(ToMicroSeconds(findData.ftLastAccessTime));
#endif

    return statistics;
}

i64 GetDirectorySize(const TString& path, bool ignoreUnavailableFiles)
{
    std::queue<TString> directories;
    directories.push(path);

    i64 size = 0;

    auto wrapNoEntryError = [&] (std::function<void()> func) {
        try {
            func();
        } catch (const TSystemError& ex) { // For util functions.
            if (ignoreUnavailableFiles && ex.Status() == ENOENT) {
                // Do nothing
            } else {
                throw;
            }
        } catch (const TErrorException& ex) { // For YT functions.
            if (ignoreUnavailableFiles && ex.Error().FindMatching(ELinuxErrorCode::NOENT)) {
                // Do nothing
            } else {
                throw;
            }
        }
    };

    while (!directories.empty()) {
        const auto& directory = directories.front();

        wrapNoEntryError([&] ()  {
            auto subdirectories = EnumerateDirectories(directory);
            for (const auto& subdirectory : subdirectories) {
                directories.push(CombinePaths(directory, subdirectory));
            }
        });

        std::vector<TString> files;
        wrapNoEntryError([&] () {
            files = EnumerateFiles(directory);
        });

        for (const auto& file : files) {
            wrapNoEntryError([&] () {
                auto fileStatistics = GetFileStatistics(CombinePaths(directory, file));
                if (fileStatistics.Size > 0) {
                    size += fileStatistics.Size;
                }
            });
        }

        directories.pop();
    }

    return size;
}

void Touch(const TString& path)
{
#ifdef _unix_
    int result = ::utimes(~path, nullptr);
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to touch %v",
            path)
            << TError::FromSystem();
    }
#else
    ThrowNotSupported();
#endif
}

namespace {

#ifdef _win_
    const char PATH_DELIM = '\\';
    const char PATH_DELIM2 = '/';
#else
    const char PATH_DELIM = '/';
    const char PATH_DELIM2 = 0;
#endif

bool IsAbsolutePath(const TString& path)
{
    if (path.empty())
        return false;
    if (path[0] == PATH_DELIM)
        return true;
#ifdef _win_
    if (path[0] == PATH_DELIM2)
        return true;
    if (path[0] > 0 && isalpha(path[0]) && path[1] == ':')
        return true;
#endif
    return false;
}

TString JoinPaths(const TString& path1, const TString& path2)
{
    if (path1.empty())
        return path2;
    if (path2.empty())
        return path1;

    auto path = path1;
    int delim = 0;
    if (path1.back() == PATH_DELIM || path1.back() == PATH_DELIM2)
        ++delim;
    if (path2[0] == PATH_DELIM || path2[0] == PATH_DELIM2)
        ++delim;
    if (delim == 0)
        path.append(1, PATH_DELIM);
    path.append(path2, delim == 2 ? 1 : 0, TString::npos);
    return path;
}

} // namespace

TString CombinePaths(const TString& path1, const TString& path2)
{
    return IsAbsolutePath(path2) ? path2 : JoinPaths(path1, path2);
}

TString CombinePaths(const std::vector<TString>& paths)
{
    YCHECK(!paths.empty());
    if (paths.size() == 1) {
        return paths[0];
    }
    auto result = CombinePaths(paths[0], paths[1]);
    for (int index = 2; index < paths.size(); ++ index) {
        result = CombinePaths(result, paths[index]);
    }
    return result;
}

TString NormalizePathSeparators(const TString& path)
{
    TString result;
    result.reserve(path.length());
    for (int i = 0; i < path.length(); ++i) {
        if (path[i] == '\\') {
            result.append('/');
        } else {
            result.append(path[i]);
        }
    }
    return result;
}

void SetExecutableMode(const TString& path, bool executable)
{
#ifdef _win_
    Y_UNUSED(path);
    Y_UNUSED(executable);
#else
    int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    if (executable) {
        mode |= S_IXOTH;
        mode |= S_IXGRP;
        mode |= S_IXUSR;
    }
    bool ok = chmod(~path, mode) == 0;
    if (!ok) {
        THROW_ERROR_EXCEPTION(
            "Failed to set mode %v for %v",
            mode,
            path)
            << TError::FromSystem();
    }
#endif
}

void MakeSymbolicLink(const TString& filePath, const TString& linkPath)
{
#ifdef _win_
    // From MSDN: If the function succeeds, the return value is nonzero.
    // If the function fails, the return value is zero. To get extended error information, call GetLastError.
    bool ok = CreateSymbolicLink(~linkPath, ~filePath, 0) != 0;
#else
    bool ok = symlink(~filePath, ~linkPath) == 0;
#endif

    if (!ok) {
        THROW_ERROR_EXCEPTION(
            "Failed to link %v to %v",
            filePath,
            linkPath)
            << TError::FromSystem();
    }
}

bool AreInodesIdentical(const TString& lhsPath, const TString& rhsPath)
{
#ifdef _unix_
    auto checkedStat = [] (const TString& path, struct stat* buffer) {
        auto result = stat(~path, buffer);
        if (result) {
            THROW_ERROR_EXCEPTION(
                "Failed to check for identical inodes: stat failed for %v",
                path)
                << TError::FromSystem();
        }
    };

    struct stat lhsBuffer, rhsBuffer;
    checkedStat(lhsPath, &lhsBuffer);
    checkedStat(rhsPath, &rhsBuffer);

    return
        lhsBuffer.st_dev == rhsBuffer.st_dev &&
        lhsBuffer.st_ino == rhsBuffer.st_ino;
#else
    return false;
#endif
}

TString GetHomePath()
{
#ifdef _win_
    std::array<char, 1024> buffer;
    SHGetSpecialFolderPath(0, buffer.data(), CSIDL_PROFILE, 0);
    return TString(buffer.data());
#else
    return std::getenv("HOME");
#endif
}

void FlushDirectory(const TString& path)
{
#ifdef _unix_
    int fd = ::open(~path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) {
        THROW_ERROR_EXCEPTION("Failed to open directory %v", path)
            << TError::FromSystem();
    }

    int result = ::fsync(fd);
    if (result < 0) {
        SafeClose(fd, false);
        THROW_ERROR_EXCEPTION("Failed to flush directory %v", path)
            << TError::FromSystem();
    }

    SafeClose(fd, false);
#else
    // No-op.
#endif
}

std::vector<TMountPoint> GetMountPoints(const TString& mountsFile)
{
#ifdef _linux_
    std::unique_ptr<FILE, decltype(&endmntent)> file(::setmntent(~mountsFile, "r"), endmntent);

    if (!file.get()) {
        THROW_ERROR_EXCEPTION("Failed to open mounts file %v", mountsFile);
    }

    std::vector<TMountPoint> mountPoints;

    ::mntent* entry;
    while ((entry = getmntent(file.get()))) {
        TMountPoint point;
        point.Name = entry->mnt_fsname;
        point.Path = entry->mnt_dir;
        mountPoints.push_back(point);
    }

    return mountPoints;
#else
    ThrowNotSupported();
    Y_UNREACHABLE();
#endif
}

void MountTmpfs(const TString& path, int userId, i64 size)
{
#ifdef _linux_
    auto opts = Format("mode=0777,uid=%v,size=%v", userId, size);
    int result = ::mount("none", ~path, "tmpfs", 0, ~opts);
    if (result < 0) {
        THROW_ERROR_EXCEPTION("Failed to mount tmpfs at %v", path)
            << TErrorAttribute("user_id", userId)
            << TErrorAttribute("size", size)
            << TError::FromSystem();
    }
#else
    ThrowNotSupported();
#endif
}

void Umount(const TString& path, bool detach)
{
#ifdef _linux_
    int flags = 0;
    if (detach) {
        flags |= MNT_DETACH;
    }
    int result = ::umount2(~path, flags);
    // EINVAL for ::umount means that nothing mounted at this point.
    // ENOENT means 'No such file or directory'.
    if (result < 0 && LastSystemError() != EINVAL && LastSystemError() != ENOENT) {
        auto error = TError("Failed to umount %v", path)
            << TError::FromSystem();
        if (LastSystemError() == EBUSY) {
            error = AttachLsofOutput(error, path);
            error = AttachFindOutput(error, path);
        }
        THROW_ERROR error;
    }

#else
    ThrowNotSupported();
#endif
}

struct stat Stat(const TStringBuf& path)
{
    struct stat statInfo;
    int result = ::stat(path.c_str(), &statInfo);
    if (result != 0) {
        THROW_ERROR_EXCEPTION("Failed to execute ::stat for %v", path)
            << TError::FromSystem();
    }
    return statInfo;
}

i64 GetBlockSize(const TStringBuf& device)
{
    struct stat statInfo = Stat(device);
    return static_cast<i64>(statInfo.st_blksize);
}

TString GetFilesystemName(const TStringBuf& path)
{
    struct stat statInfo = Stat(path);
    auto dev = statInfo.st_dev;

    for (const auto& mountPoint : GetMountPoints()) {
        struct stat currentStatInfo;
        if (::stat(mountPoint.Path.c_str(), &currentStatInfo) != 0) {
            continue;
        }

        if (currentStatInfo.st_dev == dev) {
            return mountPoint.Name;
        }
    }

    THROW_ERROR_EXCEPTION("Failed to find mount point for %v", path);
}

void SetQuota(
    int userId,
    const TStringBuf& path,
    TNullable<i64> diskSpaceLimit,
    TNullable<i64> inodeLimit)
{
#ifdef _linux_
    dqblk info;
    const i64 blockSize = GetBlockSize(path);
    const auto filesystem = GetFilesystemName(path);
    u_int32_t flags = 0;
    if (diskSpaceLimit) {
        const auto diskSpaceLimitValue = (*diskSpaceLimit + blockSize - 1) / blockSize;
        info.dqb_bhardlimit = static_cast<u_int64_t>(diskSpaceLimitValue);
        info.dqb_bsoftlimit = info.dqb_bhardlimit;
        flags |= QIF_BLIMITS;
    }
    if (inodeLimit) {
        info.dqb_ihardlimit = static_cast<u_int64_t>(*inodeLimit);
        info.dqb_isoftlimit = info.dqb_ihardlimit;
        flags |= QIF_ILIMITS;
    }
    info.dqb_valid = flags;
    int result = ::quotactl(
        QCMD(Q_SETQUOTA, USRQUOTA),
        filesystem.c_str(),
        userId,
        reinterpret_cast<caddr_t>(&info));
    if (result < 0) {
        THROW_ERROR_EXCEPTION("Failed to set FS quota for user")
            << TErrorAttribute("user_id", userId)
            << TErrorAttribute("disk_space_limit", diskSpaceLimit.Get(0))
            << TErrorAttribute("inode_limit", inodeLimit.Get(0))
            << TErrorAttribute("path", path)
            << TError::FromSystem();
    }
#else
    ThrowNotSupported();
#endif
}

void ExpectIOErrors(std::function<void()> func)
{
    try {
        func();
    } catch (const TSystemError& ex) {
        auto status = ex.Status();
        if (status == EIO ||
            status == ENOSPC ||
            status == EROFS)
        {
            throw;
        }
        TError error(ex);
        LOG_FATAL(error,"Unexpected exception thrown during IO operation");
    } catch (...) {
        TError error(CurrentExceptionMessage());
        LOG_FATAL(error, "Unexpected exception thrown during IO operation");
    }
}

void Chmod(const TString& path, int mode)
{
#ifdef _linux_
    int result = ::Chmod(~path, mode);
    if (result < 0) {
        THROW_ERROR_EXCEPTION("Failed to change mode of %v", path)
            << TErrorAttribute("mode", Format("%04o", mode))
            << TError::FromSystem();
    }
#else
    ThrowNotSupported();
#endif
}

void ChunkedCopy(
    const TString& existingPath,
    const TString& newPath,
    i64 chunkSize)
{
#ifdef _linux_
    try {
        TFile src(existingPath, OpenExisting | RdOnly | Seq | CloseOnExec);
        TFile dst(newPath, CreateAlways | WrOnly | Seq | CloseOnExec);
        dst.Flock(LOCK_EX);

        i64 srcSize = src.GetLength();
        if (srcSize == -1) {
            THROW_ERROR_EXCEPTION("Cannot get source file length: stat failed for %v",
                existingPath)
                << TError::FromSystem();
        }

        int srcFd = src.GetHandle();
        int dstFd = dst.GetHandle();

        while (true) {
            i64 currentChunkSize = 0;
            while (currentChunkSize < chunkSize && srcSize > 0) {
                auto size = sendfile(dstFd, srcFd, nullptr, chunkSize);
                if (size == -1) {
                    THROW_ERROR_EXCEPTION("Error while doing chunked copy: sendfile failed")
                        << TError::FromSystem();
                }
                currentChunkSize += size;
                srcSize -= size;
            }

            if (srcSize == 0) {
                break;
            }

            NConcurrency::Yield();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to copy %v to %v",
            existingPath,
            newPath)
            << ex;
    }
#else
    ThrowNotSupported();
#endif
}

TError AttachLsofOutput(TError error, const TString& path)
{
    auto lsofOutput = TShellCommand("lsof", {path})
        .Run()
        .Wait()
        .GetOutput();
    return error
        << TErrorAttribute("lsof_output", lsofOutput);
}

TError AttachFindOutput(TError error, const TString& path)
{
    auto findOutput = TShellCommand("find", {path, "-name", "*"})
        .Run()
        .Wait()
        .GetOutput();
    return error
        << TErrorAttribute("find_output", findOutput);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS

////////////////////////////////////////////////////////////////////////////////

i64 TGetDirectorySizeAsRootTool::operator()(const TString& path) const
{
    SafeSetUid(0);
    return NFS::GetDirectorySize(path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
