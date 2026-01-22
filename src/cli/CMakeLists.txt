add_executable(dc_cli
    api_client.cpp
    formatters.cpp
    main.cpp
)

target_include_directories(dc_cli
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${PROJECT_SOURCE_DIR}/third_party
)

target_link_libraries(dc_cli
    PRIVATE
        dc_common
)
