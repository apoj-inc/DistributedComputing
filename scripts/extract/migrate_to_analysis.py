# migrations/20250315_01_initial.py
from yoyo import step

__depends__ = {}

steps = [
    step(
        """
        CREATE TABLE IF NOT EXISTS quartus_task_results (
            task_id              BIGINT PRIMARY KEY,
            project_name         TEXT NOT NULL,
            revision_name        TEXT,
            qar_filename         TEXT NOT NULL,
            status               TEXT NOT NULL,
            compiled_at          TIMESTAMPTZ,
            top_fmax_mhz         DOUBLE PRECISION,
            raw_fmax_json        JSONB NOT NULL DEFAULT '[]'::jsonb,
            raw_summary_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS quartus_task_parameters (
            task_id                       BIGINT PRIMARY KEY
                REFERENCES quartus_task_results(task_id) ON DELETE CASCADE,

            -- Existing parameters
            axi_data_width                BIGINT NOT NULL DEFAULT 32,
            axi_id_w_width                BIGINT NOT NULL DEFAULT 5,
            axi_id_r_width                BIGINT NOT NULL DEFAULT 5,
            axi_addr_width                BIGINT NOT NULL DEFAULT 16,
            axis_data_width               BIGINT NOT NULL DEFAULT 40,
            axis_id_width                 BIGINT NOT NULL DEFAULT 3,
            axis_dest_width               BIGINT NOT NULL DEFAULT 4,
            axis_user_width               BIGINT NOT NULL DEFAULT 4,
            axi_master_loader_fifo_depth  BIGINT NOT NULL DEFAULT 64,
            max_routers_x                 BIGINT NOT NULL DEFAULT 4,
            max_routers_y                 BIGINT NOT NULL DEFAULT 4,
            buffer_depth                  BIGINT NOT NULL DEFAULT 16,
            algorithm                     TEXT NOT NULL DEFAULT 'XY',
            routers_count                 BIGINT NOT NULL,
            core_count                    BIGINT NOT NULL,
            axi_max_id_width              BIGINT NOT NULL,
            axi_data_bytes                BIGINT NOT NULL,
            defines_json                  JSONB NOT NULL DEFAULT '{}'::jsonb,

            -- New parameters
            topology                      TEXT NOT NULL DEFAULT 'Mesh',
            buffer_allocator              TEXT NOT NULL DEFAULT 'Straight',
            generatics_count              BIGINT NOT NULL DEFAULT 2,
            generatics                    JSONB NOT NULL DEFAULT '[]'::jsonb,
            virtual_channel_number        BIGINT NOT NULL DEFAULT 2,
            simultanious_virtual_network_routing BIGINT NOT NULL DEFAULT 1,
            virtual_network_number        BIGINT NOT NULL DEFAULT 2,
            virtual_networks              JSONB NOT NULL DEFAULT '[]'::jsonb
        );

        CREATE TABLE IF NOT EXISTS quartus_entity_utilization (
            task_id              BIGINT NOT NULL,
            entity_id            BIGINT NOT NULL,
            parent_entity_id     BIGINT,
            entity_name          TEXT NOT NULL,
            entity_path          TEXT NOT NULL,
            raw_names_json       JSONB NOT NULL DEFAULT '[]'::jsonb,
            source_panels_json   JSONB NOT NULL DEFAULT '[]'::jsonb,
            alms                 BIGINT,
            registers            BIGINT,
            memory_bits          BIGINT,
            metrics_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (task_id, entity_id),
            UNIQUE (task_id, entity_path),
            FOREIGN KEY (task_id) REFERENCES quartus_task_results(task_id) ON DELETE CASCADE,
            FOREIGN KEY (task_id, parent_entity_id)
                REFERENCES quartus_entity_utilization(task_id, entity_id)
                DEFERRABLE INITIALLY DEFERRED
        );
        """,
        """
        DROP TABLE IF EXISTS quartus_entity_utilization;
        DROP TABLE IF EXISTS quartus_task_parameters;
        DROP TABLE IF EXISTS quartus_task_results;
        """
    )
]