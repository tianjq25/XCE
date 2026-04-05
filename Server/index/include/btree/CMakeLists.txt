cmake_minimum_required(VERSION 3.12 FATAL_ERROR)
set(CMAKE_BUILD_TYPE "Release")
# set(SGX_MODE "Debug")
set(CMAKE_C_COMPILER /usr/bin/gcc)
set(CMAKE_CXX_COMPILER /usr/bin/g++)

# use sgx-cmake module
list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

project(b_tree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/build)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/build)
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")

find_library(SSL_LIB NAMES ssl PATHS /lib/x86_64-linux-gnu NO_DEFAULT_PATH)
find_library(CRYPTO_LIB NAMES crypto PATHS /lib/x86_64-linux-gnu NO_DEFAULT_PATH)

include_directories(stx-btree-0.9/include 
                atomic_queue/
                utils/
                buffer/)

add_executable(ycsb ycsb.cpp)

target_link_libraries(ycsb ${SSL_LIB} ${CRYPTO_LIB} rt)


add_custom_command(
    OUTPUT ${CMAKE_CURRENT_LIST_DIR}/ipc.manifest.sgx ${CMAKE_CURRENT_LIST_DIR}/ipc.sig ${CMAKE_CURRENT_LIST_DIR}/ipc.manifest
    COMMAND gramine-manifest -Dexecdir=${CMAKE_CURRENT_LIST_DIR} -Denclave_size=8192M ${CMAKE_CURRENT_LIST_DIR}/ipc.manifest.template ${CMAKE_CURRENT_LIST_DIR}/ipc.manifest
    COMMAND gramine-sgx-sign --manifest ${CMAKE_CURRENT_LIST_DIR}/ipc.manifest --output ${CMAKE_CURRENT_LIST_DIR}/ipc.manifest.sgx
    DEPENDS ycsb
)

add_custom_target(manifest ALL  
    DEPENDS ${CMAKE_CURRENT_LIST_DIR}/ipc.manifest.sgx
)
