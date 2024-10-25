# - Find linux-headers-generic
#
# LINUX_HEADERS_GENERIC_INCLUDE
# LINUX_HEADERS_GENERIC_LIBS 
# LINUX_HEADERS_GENERIC_FOUND

if(NOT TARGET linux-headers-generic::linux-headers-generic)
    add_library(linux-headers-generic::linux-headers-generic INTERFACE IMPORTED)
    target_include_directories(linux-headers-generic::linux-headers-generic INTERFACE /workspaces/ytsaurus/contrib/libs/linux-headers)
    # target_include_directories(glog::glog INTERFACE ${GLOG_INCLUDE_DIRS})
    # target_link_libraries(glog::glog INTERFACE ${GLOG_LIBRARY})
endif()
