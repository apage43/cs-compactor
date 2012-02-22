cmake_minimum_required(VERSION 2.8)
project (CSCW)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/")
find_package(EI REQUIRED)

include_directories(${EI_INCLUDE_DIRS})
set(libs ${LIBS} ${EI_LIBRARIES})
add_executable(compactor
        src/compactor.cc
        src/wrap.cc
        src/reduces.cc
        src/btree_copy.cc
        src/mergesor.c
        src/llmsort.c
    )

target_link_libraries(compactor couchstore ${LIBS})
