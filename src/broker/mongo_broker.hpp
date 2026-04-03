#pragma once

#include <mongocxx/v_noabi/mongocxx/client.hpp>
#include <mongocxx/v_noabi/mongocxx/client_session.hpp>
#include <mongocxx/v_noabi/mongocxx/collection.hpp>
#include <mongocxx/v_noabi/mongocxx/database.hpp>
#include <mongocxx/v_noabi/mongocxx/instance.hpp>

#include "broker.hpp"

namespace dc {
namespace broker {

class MongoBroker final : public Broker {
public:
    explicit MongoBroker(DbConfig& config);

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
    std::int64_t NextTaskId(mongocxx::client_session& session);
    std::int64_t NextTaskId();
    std::int64_t CreateTaskNoTransaction(const TaskInput& task);
    std::optional<std::vector<TaskDispatch>> PollTasksForAgentNoTransaction(
        const std::string& agent_id,
        int free_slots);
    bool UpdateTaskStatusNoTransaction(std::int64_t task_id,
                                       TaskState state,
                                       const std::optional<int>& exit_code,
                                       const std::optional<std::string>& started_at,
                                       const std::optional<std::string>& finished_at,
                                       const std::optional<std::string>& error_message);
    CancelTaskResult CancelTaskNoTransaction(std::int64_t task_id);
    int MarkOfflineAgentsAndRequeueNoTransaction(int offline_after_sec);

    mongocxx::instance& mongo_instance_;
    mongocxx::client client_;
    mongocxx::database db_;
    mongocxx::collection agents_;
    mongocxx::collection tasks_;
    mongocxx::collection task_assignments_;
    mongocxx::collection counters_;
};

}  // namespace broker
}  // namespace dc
