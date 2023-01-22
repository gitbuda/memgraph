set(storage_rocks_src_files
    storage.cpp)

find_package(Threads REQUIRED)

add_library(mg-storage-rocks STATIC ${storage_rocks_src_files})
target_link_libraries(mg-storage-rocks Threads::Threads mg-utils)
