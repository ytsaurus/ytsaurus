#pragma once

/*!
 * \file fs.h
 * \brief File system functions
 */

#include "common.h"

#include <yt/core/actions/public.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NFS {

////////////////////////////////////////////////////////////////////////////////

//! File suffix for temporary files.
const char* const TempFileSuffix = "~";

//! Returns |true| if a given path points to an existing file or directory.
bool Exists(const TString& path);

//! Removes a given file or directory.
void Remove(const TString& path);

//! Removes #destination if it exists. Then renames #destination into #source.
void Replace(const TString& source, const TString& destination);

//! Removes a given directory recursively.
void RemoveRecursive(const TString& path);

//! Renames a given file or directory.
void Rename(const TString& source, const TString& destination);

//! Returns name of file.
TString GetFileName(const TString& path);

//! Returns extension of file.
TString GetFileExtension(const TString& path);

//! Returns name of file without extension.
TString GetFileNameWithoutExtension(const TString& path);

//! Returns path of directory containing the file.
TString GetDirectoryName(const TString& path);

//! Returns the absolute path for the given (possibly relative) path.
TString GetRealPath(const TString& path);

//! Checks that given path is relative and does not go outside the directory.
bool CheckPathIsRelativeAndGoesInside(const TString& path);

//! Combines two strings into a path.
TString CombinePaths(const TString& path1, const TString& path2);

//! Combines a bunch of strings into a path.
TString CombinePaths(const std::vector<TString>& paths);

//! Deletes all files with extension #TempFileSuffix in a given directory.
void CleanTempFiles(const TString& path);

//! Returns all files in a given directory.
std::vector<TString> EnumerateFiles(const TString& path, int depth = 1);

//! Returns all directories in a given directory.
std::vector<TString> EnumerateDirectories(const TString& path, int depth = 1);

//! Describes total, free, and available space on a disk drive.
struct TDiskSpaceStatistics
{
    i64 TotalSpace;
    i64 FreeSpace;
    i64 AvailableSpace;
};

//! Computes the space statistics for disk drive containing #path.
TDiskSpaceStatistics GetDiskSpaceStatistics(const TString& path);

//! Creates the #path and parent directories if they don't exists.
void MakeDirRecursive(const TString& path, int mode = 0777);

struct TFileStatistics
{
    i64 Size = -1;
    TInstant ModificationTime;
    TInstant AccessTime;
};

//! Returns the file statistics.
TFileStatistics GetFileStatistics(const TString& path);

//! Recursively calculates size of all regular files inside the directory.
i64 GetDirectorySize(const TString& path, bool ignoreUnavailableFiles = true);

//! Sets the access and modification times to now.
void Touch(const TString& path);

//! Converts all back slashes to forward slashes.
TString NormalizePathSeparators(const TString& path);

//! Sets 'executable' mode.
void SetExecutableMode(const TString& path, bool executable);

//! Makes a symbolic link on file #fileName with #linkName.
void MakeSymbolicLink(const TString& filePath, const TString& linkPath);

//! Returns |true| if given paths refer to the same inode.
//! Always returns |false| under Windows.
bool AreInodesIdentical(const TString& lhsPath, const TString& rhsPath);

//! Returns the home directory of the current user.
//! Interestingly, implemented for both Windows and *nix.
TString GetHomePath();

//! Flushes the directory's metadata. Useful for, e.g., committing renames happened in #path.
void FlushDirectory(const TString& path);

struct TMountPoint
{
    TString Name;
    TString Path;
};

std::vector<TMountPoint> GetMountPoints(const TString& mountsFile = "/proc/mounts");

//! Mount tmpfs at given path.
void MountTmpfs(const TString& path, int userId, i64 size);

//! Unmount given path.
void Umount(const TString& path, bool detach);

//! Set disk space and inodes quota for given user on filesystem determined by pathInFs.
//! The filesystem must be mounted with quotas enabled.
void SetQuota(
    int userId,
    TStringBuf path,
    std::optional<i64> diskSpaceLimit,
    std::optional<i64> inodeLimit);

//! Wraps a given #func in with try/catch; makes sure that only IO-related
//! exceptions are being thrown. For all other exceptions, immediately terminates
//! with fatal error.
void ExpectIOErrors(std::function<void()> func);

//! Sets a given mode on the path.
void Chmod(const TString& path, int mode);

//! Copies file chunk after chunk, releasing thread between chunks.
void ChunkedCopy(
    const TString& existingPath, 
    const TString& newPath, 
    i64 chunkSize);

TError AttachLsofOutput(TError error, const TString& path);
TError AttachFindOutput(TError error, const TString& path);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFS

////////////////////////////////////////////////////////////////////////////////

struct TGetDirectorySizeAsRootTool
{
    i64 operator()(const TString& path) const;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
