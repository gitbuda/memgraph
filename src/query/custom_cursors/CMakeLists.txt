set(mg_custom_cursors_sources
  create_node.cpp
  once.cpp
  scanall.cpp
  produce.cpp
)
add_library(mg_custom_cursors STATIC ${mg_custom_cursors_sources})
# TODO(gitbuda): Having mg-storage-v2 here is broken.
target_link_libraries(mg_custom_cursors mg-utils mg-storage-v2 mg_custom_storage mg-query)
