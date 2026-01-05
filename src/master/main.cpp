#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#if !defined(_WIN32)
#include <sys/wait.h>
#endif

#include "common/env.h"
#include "common/logging.h"
#include "control_service.h"

namespace {

int NormalizeExitCode(int code) {
#if defined(_WIN32)
    return code;
#else
    if (WIFEXITED(code)) {
        return WEXITSTATUS(code);
    }
    return code;
#endif
}

bool IsCommandNotFound(int code) {
    return code == 127 || code == 9009;
}

int RunInitDbScript() {
    const std::string config_path = dc::common::GetEnvOrDefault("DB_CONFIG", "");
    const std::string preferred_python = dc::common::GetEnvOrDefault("INIT_DB_PYTHON", "");

    std::vector<std::string> candidates;
    if (!preferred_python.empty()) {
        candidates.push_back(preferred_python);
    } else {
        candidates.push_back("python3");
        candidates.push_back("python");
    }

    for (const auto& python_cmd : candidates) {
        std::string command = python_cmd + " scripts/init_db.py";
        if (!config_path.empty()) {
            command += " --config \"" + config_path + "\"";
        }

        spdlog::info("Running DB init: {}", command);
        int raw_code = std::system(command.c_str());
        int code = NormalizeExitCode(raw_code);

        if (IsCommandNotFound(code) && candidates.size() > 1) {
            continue;
        }
        return code;
    }

    return 127;
}

}  // namespace

int main() {
    using namespace dc::master;

    const std::string log_dir = dc::common::GetEnvOrDefault("LOG_DIR", "logs");
    const std::string log_level =
        dc::common::GetEnvOrDefault("MASTER_LOG_LEVEL", "trace");
    const std::string log_file =
        dc::common::GetEnvOrDefault("MASTER_LOG_FILE", log_dir + "/master.log");
    const auto resolved_level =
        dc::common::ParseLogLevel(log_level, spdlog::level::trace);
    dc::common::InitLogging(log_file, resolved_level);
    spdlog::info("Logging initialized (level={}, file={})",
                 spdlog::level::to_string_view(resolved_level),
                 log_file);

    // Configuration is pulled from environment variables to match deployment style.
    MasterConfig config;
    config.host = dc::common::GetEnvOrDefault("MASTER_HOST", "0.0.0.0");
    config.port = dc::common::GetEnvIntOrDefault("MASTER_PORT", 8080);
    config.heartbeat_interval_sec = dc::common::GetEnvIntOrDefault("HEARTBEAT_SEC", 30);
    config.offline_after_sec = dc::common::GetEnvIntOrDefault("OFFLINE_SEC", 120);
    config.log_dir = log_dir;

    DbConfig db;
    db.host = dc::common::GetEnvOrDefault("DB_HOST", "localhost");
    db.port = dc::common::GetEnvOrDefault("DB_PORT", "5432");
    db.user = dc::common::GetEnvOrDefault("DB_USER", "");
    db.password = dc::common::GetEnvOrDefault("DB_PASSWORD", "");
    db.dbname = dc::common::GetEnvOrDefault("DB_NAME", "");
    db.sslmode = dc::common::GetEnvOrDefault("DB_SSLMODE", "");

    if (db.user.empty() || db.dbname.empty()) {
        spdlog::critical("Missing DB_USER or DB_NAME environment variable.");
        return 2;
    }

    // Ensure log root exists even if no task logs are present yet.
    std::error_code ec;
    std::filesystem::create_directories(config.log_dir, ec);
    if (ec) {
        spdlog::error("Failed to create LOG_DIR: {}", config.log_dir);
        return 2;
    }

    // Apply/init DB schema; refuse to start if init_db reports differences.
    int init_code = RunInitDbScript();
    if (init_code == 4) {
        spdlog::critical("Database schema mismatch; see init_db.py output above.");
        return init_code;
    }
    if (init_code != 0) {
        spdlog::critical("Database init failed with code {}", init_code);
        return init_code;
    }

    Storage storage(db);
    LogStore log_store(config.log_dir);
    ControlService service(config, storage, log_store);
    return service.Run();
}
