#pragma once

#include <memory>
#include <string>
#include <vector>

#include "agent_client.h"
#include "task_executor.h"

namespace dc {
namespace worker {

struct WorkerConfig {
    std::string master_url;
    std::string agent_id;
    std::string os;
    std::string version;
    int cpu_cores = 0;
    int ram_mb = 0;
    int slots = 1;
    int http_timeout_ms = 5000;
    std::string log_dir;
    int cancel_check_interval_sec = 1;
};

WorkerConfig LoadWorkerConfigFromEnv();
bool ValidateWorkerConfig(const WorkerConfig& config, std::string* error);

class WorkerApp {
public:
    WorkerApp(WorkerConfig config, std::unique_ptr<AgentClient> client);

    // Runs the agent; when run_once is true, performs a single heartbeat+poll cycle.
    int Run(bool run_once);

private:
    bool Register();
    void TickOnce();

    WorkerConfig config_;
    std::unique_ptr<AgentClient> client_;
    int heartbeat_interval_sec_ = 10;
    TaskExecutor executor_;
};

// Собирает WorkerApp: читает конфигурацию из окружения, валидирует, создаёт клиент.
std::unique_ptr<WorkerApp> CreateWorkerAppFromEnv(std::string* error);

}  // namespace worker
}  // namespace dc
