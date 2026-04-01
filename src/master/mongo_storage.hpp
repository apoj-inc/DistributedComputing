#pragma once

#include <mongocxx/client.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>

#include "storage.hpp"

namespace dc {
namespace master {

class MongoStorage final : public Storage {
public:
    explicit MongoStorage(DbConfig& config);

    bool UpsertAgent(const AgentInput& agent) override;
    bool UpdateHeartbeat(const AgentHeartbeat& heartbeat) override;

    std::optional<AgentRecord> GetAgent(const std::string& agent_id) override;
    std::vector<AgentRecord> ListAgents(const std::optional<AgentStatus>& status,
                                        int limit,
                                        int offset) override;

    std::int64_t CreateTask(const TaskInput& task) override;
    std::optional<TaskRecord> GetTask(std::int64_t task_id) override;
    std::vector<TaskSummary> ListTasks(const std::optional<TaskState>& state,
                                       const std::optional<std::string>& agent_id,
                                       int limit,
                                       int offset) override;

    std::optional<std::vector<TaskDispatch>> PollTasksForAgent(const std::string& agent_id,
                                                               int free_slots) override;

    bool UpdateTaskStatus(std::int64_t task_id,
                          TaskState state,
                          const std::optional<int>& exit_code,
                          const std::optional<std::string>& started_at,
                          const std::optional<std::string>& finished_at,
                          const std::optional<std::string>& error_message) override;

    CancelTaskResult CancelTask(std::int64_t task_id) override;
    int MarkOfflineAgentsAndRequeue(int offline_after_sec) override;

protected:
    std::string ConnectionString() const override;

private:
    void EnsureIndexes();
    std::int64_t NextTaskId(mongocxx::client_session& session);

    mongocxx::client client_;
    mongocxx::database db_;
    mongocxx::collection agents_;
    mongocxx::collection tasks_;
    mongocxx::collection task_assignments_;
    mongocxx::collection counters_;
};

}  // namespace master
}  // namespace dc
