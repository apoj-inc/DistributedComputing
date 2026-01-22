add_executable(dc_worker
    agent_client.cpp
    task_executor.cpp
    worker_app.cpp
    main.cpp
)

target_include_directories(dc_worker
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${PROJECT_SOURCE_DIR}/third_party
)

target_link_libraries(dc_worker
    PRIVATE
        dc_common
)
