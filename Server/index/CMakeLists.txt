cmake_minimum_required(VERSION 3.12)
set(CMAKE_C_COMPILER /usr/bin/gcc)
set(CMAKE_CXX_COMPILER /usr/bin/g++)

project(DEMO)
set(CMAKE_BUILD_TYPE Debug)
# set(CMAKE_BUILD_TYPE Release)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_C_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_library(SSL_LIB NAMES ssl PATHS /lib/x86_64-linux-gnu NO_DEFAULT_PATH)
find_library(CRYPTO_LIB NAMES crypto PATHS /lib/x86_64-linux-gnu NO_DEFAULT_PATH)

include_directories(include utils $ENV{HOME}/.local/include)
file(GLOB_RECURSE SRC_FILES "include/*.cpp" "utils/*.cpp")


add_executable(table_test table_test.cpp ${SRC_FILES})
add_executable(demo demo.cpp ${SRC_FILES})

target_link_libraries(demo ${SSL_LIB} ${CRYPTO_LIB} rt)
target_link_libraries(table_test ${SSL_LIB} ${CRYPTO_LIB} rt)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_LIST_DIR}/demo.manifest.sgx ${CMAKE_CURRENT_LIST_DIR}/demo.sig ${CMAKE_CURRENT_LIST_DIR}/demo.manifest
    COMMAND gramine-manifest -Dexecdir=${CMAKE_CURRENT_LIST_DIR} -Denclave_size=8192M ${CMAKE_CURRENT_LIST_DIR}/demo.manifest.template ${CMAKE_CURRENT_LIST_DIR}/demo.manifest
    COMMAND gramine-sgx-sign --manifest ${CMAKE_CURRENT_LIST_DIR}/demo.manifest --output ${CMAKE_CURRENT_LIST_DIR}/demo.manifest.sgx
    DEPENDS demo
)

add_custom_target(manifest ALL  
    DEPENDS ${CMAKE_CURRENT_LIST_DIR}/demo.manifest.sgx
)
