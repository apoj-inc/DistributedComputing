cmake_minimum_required(VERSION 3.20)

project(
    dc_master
    VERSION 1.0
    LANGUAGES CXX
)

file(GLOB_RECURSE PROJECT_SRC CONFIGURE_DEPENDS "*.cpp")
set(PROJECT_INC "dc_common")
set(PROJECT_LIB "dc_common")

list(APPEND PROJECT_INC ${CMAKE_CURRENT_SOURCE_DIR})
list(APPEND PROJECT_INC ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(${PROJECT_NAME} ${PROJECT_SRC})

target_include_directories(${PROJECT_NAME} PRIVATE ${PROJECT_INC})

find_package(Threads REQUIRED)
find_package(PostgreSQL REQUIRED)

include(FetchContent)

set(ENABLE_TESTS OFF CACHE BOOL "Disable mongo-cxx-driver tests" FORCE)
set(ENABLE_STATIC ON CACHE BOOL "Build static mongo-cxx-driver libs" FORCE)
set(ENABLE_SHARED OFF CACHE BOOL "Disable shared mongo-cxx-driver libs" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static libs in third-party deps" FORCE)

FetchContent_Declare(
    mongo-cxx-driver
    GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver.git
    GIT_TAG r4.2.0
    GIT_SHALLOW true
)
FetchContent_MakeAvailable(mongo-cxx-driver)

FetchContent_Declare(
    libpqxx
    GIT_REPOSITORY https://github.com/jtv/libpqxx.git
    GIT_TAG fd7758fcdbf1bc7278d85d525de061acfb412d94
    GIT_SHALLOW true
)
FetchContent_MakeAvailable(libpqxx)

list(APPEND PROJECT_LIB Threads::Threads)
list(APPEND PROJECT_LIB libpqxx::pqxx)
list(APPEND PROJECT_LIB bsoncxx_static)
list(APPEND PROJECT_LIB mongocxx_static)

target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_LIB})
