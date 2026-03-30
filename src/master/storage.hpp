#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "status.hpp"

namespace dc {
namespace master {

struct DbConfig {
    std::string host;
    std::string port;
    std::string user;
    std::string password;
    std::string dbname;
    std::string sslmode;
};

struct AgentInput {
    std::string agent_id;
    std::string os;
    std::string version;
    int cpu_cores = 0;
    int ram_mb = 0;
    int slots = 0;
};

struct AgentHeartbeat {
    std::string agent_id;
    AgentStatus status = AgentStatus::Idle;
};

struct AgentRecord {
    std::string agent_id;
    std::string os;
    std::string version;
    int cpu_cores = 0;
    int ram_mb = 0;
    int slots = 0;
    AgentStatus status = AgentStatus::Idle;
    std::string last_heartbeat;
};

struct TaskInput {
    std::string command;
    nlohmann::json args;
    nlohmann::json env;
    std::optional<int> timeout_sec;
    nlohmann::json constraints;
};

struct TaskRecord {
    std::int64_t task_id = 0;
    TaskState state = TaskState::Queued;
    std::string command;
    nlohmann::json args;
    nlohmann::json env;
    std::optional<int> timeout_sec;
    std::optional<std::string> assigned_agent;
    std::string created_at;
    std::optional<std::string> started_at;
    std::optional<std::string> finished_at;
    std::optional<int> exit_code;
    std::optional<std::string> error_message;
    nlohmann::json constraints;
};

struct TaskSummary {
    std::int64_t task_id = 0;
    TaskState state = TaskState::Queued;
};

struct TaskDispatch {
    std::int64_t task_id = 0;
    std::string command;
    nlohmann::json args;
    nlohmann::json env;
    std::optional<int> timeout_sec;
    nlohmann::json constraints;
};

enum class CancelTaskResult {
    Ok,
    NotFound,
    InvalidState,
    Error,
};

class Storage {
public:
    explicit Storage(DbConfig config);

    bool UpsertAgent(const AgentInput& agent);
    bool UpdateHeartbeat(const AgentHeartbeat& heartbeat);

    std::optional<AgentRecord> GetAgent(const std::string& agent_id);
    std::vector<AgentRecord> ListAgents(const std::optional<AgentStatus>& status,
                                        int limit,
                                        int offset);

    std::int64_t CreateTask(const TaskInput& task);
    std::optional<TaskRecord> GetTask(std::int64_t task_id);
    std::vector<TaskSummary> ListTasks(const std::optional<TaskState>& state,
                                       const std::optional<std::string>& agent_id,
                                       int limit,
                                       int offset);

    // Returns std::nullopt when agent does not exist.
    std::optional<std::vector<TaskDispatch>> PollTasksForAgent(const std::string& agent_id,
                                                               int free_slots);

    bool UpdateTaskStatus(std::int64_t task_id,
                          TaskState state,
                          const std::optional<int>& exit_code,
                          const std::optional<std::string>& started_at,
                          const std::optional<std::string>& finished_at,
                          const std::optional<std::string>& error_message);

    CancelTaskResult CancelTask(std::int64_t task_id);

    // Marks agents offline and requeues tasks assigned to them.
    int MarkOfflineAgentsAndRequeue(int offline_after_sec);

private:
    std::string ConnectionString() const;

    DbConfig config_;
};

}  // namespace master
}  // namespace dc
