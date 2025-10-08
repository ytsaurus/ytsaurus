# MlockFileMappings

MlockFileMappings loads and locks all pages of the executable file into memory instantly.

Unlike the mlockall call, the function does not lock other pages of the process. Instead, it allocates physical memory for only the Memory Mapping Objects (vma) associated with the executable file. A typical process first starts and initializes the allocator, then calls the function to mlock the pages of the executable file. The allocator at startup allocates large ranges using mmap, but it doesn't actually use them until the mlockFileMappings call, resulting in increased memory consumption.

Additionally, unlike mlockall, the function can load pages into memory immediately without waiting for page faults to occur.
