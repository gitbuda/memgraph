set(mg_custom_storage_sources
  storage.cpp
)
add_library(mg_custom_storage STATIC ${mg_custom_storage_sources})
target_link_libraries(mg_custom_storage mg-utils)
