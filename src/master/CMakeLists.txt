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
find_package(bsoncxx REQUIRED)
find_package(mongocxx REQUIRED)

include(FetchContent)

FetchContent_Declare(
    libpqxx
    GIT_REPOSITORY https://github.com/jtv/libpqxx.git
    GIT_TAG fd7758fcdbf1bc7278d85d525de061acfb412d94
    GIT_SHALLOW true
)
FetchContent_MakeAvailable(libpqxx)

list(APPEND PROJECT_LIB Threads::Threads)
list(APPEND PROJECT_LIB libpqxx::pqxx)

if(TARGET mongo::mongocxx_shared)
    list(APPEND PROJECT_LIB mongo::mongocxx_shared)
elseif(TARGET mongocxx_shared)
    list(APPEND PROJECT_LIB mongocxx_shared)
elseif(TARGET mongocxx::mongocxx)
    list(APPEND PROJECT_LIB mongocxx::mongocxx)
else()
    message(FATAL_ERROR "Could not find mongocxx CMake target")
endif()

if(TARGET mongo::bsoncxx_shared)
    list(APPEND PROJECT_LIB mongo::bsoncxx_shared)
elseif(TARGET bsoncxx_shared)
    list(APPEND PROJECT_LIB bsoncxx_shared)
elseif(TARGET bsoncxx::bsoncxx)
    list(APPEND PROJECT_LIB bsoncxx::bsoncxx)
else()
    message(FATAL_ERROR "Could not find bsoncxx CMake target")
endif()

target_link_libraries(${PROJECT_NAME} PRIVATE ${PROJECT_LIB})
