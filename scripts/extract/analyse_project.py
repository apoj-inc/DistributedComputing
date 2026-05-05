#!/usr/bin/env python3
import argparse
import json
import pathlib
import re
import shutil
import subprocess
import sys

import gridfs
import psycopg
from pymongo import MongoClient


def run(cmd, cwd=None):
    print("RUN:", " ".join(str(x) for x in cmd), flush=True)
    subprocess.run([str(x) for x in cmd], cwd=str(cwd) if cwd else None, check=True)


def download_qar(mongo_uri: str, mongo_db: str, gridfs_bucket: str, task_id: int, dst_path: pathlib.Path) -> str:
    client = MongoClient(mongo_uri)
    db = client[mongo_db]
    fs = gridfs.GridFS(db, collection=gridfs_bucket)
    filename = f"mesh_{task_id}.qar"
    grid_out = fs.find_one({"filename": filename})
    if grid_out is None:
        raise FileNotFoundError(f"GridFS file not found: {filename}")
    with open(dst_path, "wb") as f:
        f.write(grid_out.read())
    return filename


def restore_qar(quartus_sh: str, qar_path: pathlib.Path, restore_dir: pathlib.Path):
    restore_dir.mkdir(parents=True, exist_ok=True)
    run([quartus_sh, "--restore", str(qar_path), "-output", str(restore_dir)])


def find_project_qpf(restored_dir: pathlib.Path) -> pathlib.Path:
    qpf_files = list(restored_dir.rglob("*.qpf"))
    if not qpf_files:
        raise FileNotFoundError("No .qpf found after restore")
    qpf_files.sort(key=lambda p: (len(p.parts), str(p)))
    return qpf_files[0]


def parse_project_name(qpf_path: pathlib.Path) -> str:
    return qpf_path.stem


def run_report_extract(quartus_sh: str, tcl_path: str, project_dir: pathlib.Path, project_name: str, out_json: pathlib.Path):
    run([quartus_sh, "-t", tcl_path, project_name, str(out_json)], cwd=project_dir)


def run_fmax_extract(quartus_sta: str, tcl_path: str, project_dir: pathlib.Path, project_name: str, out_json: pathlib.Path):
    run([quartus_sta, "-t", tcl_path, project_name, str(out_json)], cwd=project_dir)


def parse_float_maybe(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    m = re.search(r"[-+]?\d+(?:\.\d+)?", str(value))
    return float(m.group(0)) if m else None


def parse_int_maybe(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    s = str(value).strip()
    if not s or s == "-":
        return None
    m = re.search(r"-?\d[\d,]*", s)
    if not m:
        return None
    return int(m.group(0).replace(",", ""))


def normalize_metric_key(key: str) -> str:
    k = (key or "").strip().lower()
    k = k.replace(",", "")
    k = re.sub(r"\s+", " ", k)
    return k


def extract_entity_resource_columns(metrics: dict):
    normalized = {normalize_metric_key(k): v for k, v in (metrics or {}).items()}

    alms = None
    registers = None
    memory_bits = None

    alm_candidates = [
        "alms",
        "alm",
        "adaptive logic modules",
        "logic utilization (in alms)",
        "logic utilization",
        "total alms",
        "logic cells",
        "logic elements",
        "les",
        "combinational aluts",
        "aluts",
    ]
    reg_candidates = [
        "registers",
        "total registers",
        "dedicated logic registers",
        "logic registers",
    ]
    mem_candidates = [
        "memory bits",
        "total memory bits",
        "block memory bits",
        "ram bits",
    ]

    for key in alm_candidates:
        if key in normalized:
            alms = parse_int_maybe(normalized[key])
            if alms is not None:
                break

    for key in reg_candidates:
        if key in normalized:
            registers = parse_int_maybe(normalized[key])
            if registers is not None:
                break

    for key in mem_candidates:
        if key in normalized:
            memory_bits = parse_int_maybe(normalized[key])
            if memory_bits is not None:
                break

    return alms, registers, memory_bits


def normalize_pipe_path(entity_path: str) -> str:
    s = (entity_path or "").strip()
    if not s:
        return ""
    if not s.startswith("|"):
        s = "|" + s
    if not s.endswith("|"):
        s = s + "|"
    return s


def split_pipe_path(path: str):
    return [p for p in normalize_pipe_path(path).split("|") if p]


def join_pipe_parts(parts):
    if not parts:
        return ""
    return "|" + "|".join(parts) + "|"


def split_label_tokens(label: str):
    s = (label or "").strip().strip("|").strip()
    if not s:
        return []

    if ":" in s:
        prefix, rest = s.split(":", 1)
        rest_parts = [p.strip() for p in rest.split(".") if p.strip()]
        if not rest_parts:
            return [s]
        return [f"{prefix}:{rest_parts[0]}"] + rest_parts[1:]

    if "." in s:
        return [p.strip() for p in s.split(".") if p.strip()]

    return [s]


def row_display_name(row: dict) -> str:
    label = (row.get("entity_label_raw") or row.get("entity_path") or "").strip()
    if not label:
        return ""
    trimmed = label.strip()
    if "|" in trimmed:
        trimmed = trimmed.strip("|").strip()
    return trimmed if trimmed else (row.get("entity_path") or "").strip().strip("|")


def normalize_fmax(raw_fmax_json):
    normalized = []
    for item in raw_fmax_json:
        raw = item.get("raw", "") if isinstance(item, dict) else str(item)
        mhz = parse_float_maybe(raw)
        normalized.append({"raw": raw, "fmax_mhz": mhz})
    return normalized


def rebuild_entities_hybrid(report_entities):
    nodes = {}
    order_counter = 0

    def ensure_node(path, name=None):
        nonlocal order_counter
        path = normalize_pipe_path(path)
        if not path:
            return None
        if path not in nodes:
            order_counter += 1
            parts = split_pipe_path(path)
            nodes[path] = {
                "entity_path": path,
                "entity_name": name if name else (parts[-1] if parts else path.strip("|")),
                "raw_names_json": [],
                "source_panels_json": [],
                "alms": None,
                "registers": None,
                "memory_bits": None,
                "metrics_json": {},
                "order": order_counter,
            }
        elif name and (not nodes[path]["entity_name"] or nodes[path]["entity_name"] == path.strip("|")):
            nodes[path]["entity_name"] = name
        return nodes[path]

    grouped = {}
    for row in report_entities:
        panel = row.get("source_panel", "")
        grouped.setdefault(panel, []).append(row)

    for panel, rows in grouped.items():
        rows = sorted(rows, key=lambda r: int(r.get("row_index", 0) or 0))
        stack = []

        for row in rows:
            indent = int(row.get("entity_indent", 0) or 0)
            display_name = row_display_name(row)
            raw_name = (row.get("entity_path") or "").strip()
            metrics = row.get("metrics", {}) or {}

            while stack and stack[-1]["indent"] >= indent:
                stack.pop()

            parent_full_path = stack[-1]["full_path"] if stack else None
            label_tokens = split_label_tokens(display_name)

            if not label_tokens:
                continue

            parent_parts = split_pipe_path(parent_full_path) if parent_full_path else []
            current_paths = []

            for i in range(len(label_tokens)):
                full_parts = parent_parts + label_tokens[: i + 1]
                full_path = join_pipe_parts(full_parts)
                current_paths.append(full_path)

                node = ensure_node(full_path, label_tokens[i])
                if panel and panel not in node["source_panels_json"]:
                    node["source_panels_json"].append(panel)

            leaf_path = current_paths[-1]
            leaf_node = ensure_node(leaf_path, label_tokens[-1])

            if raw_name and raw_name not in leaf_node["raw_names_json"]:
                leaf_node["raw_names_json"].append(raw_name)

            leaf_node["metrics_json"].update(metrics)

            alms, registers, memory_bits = extract_entity_resource_columns(metrics)
            if alms is not None:
                leaf_node["alms"] = alms
            if registers is not None:
                leaf_node["registers"] = registers
            if memory_bits is not None:
                leaf_node["memory_bits"] = memory_bits

            stack.append({
                "indent": indent,
                "full_path": leaf_path,
            })

    paths = sorted(nodes.keys(), key=lambda p: (len(split_pipe_path(p)), nodes[p]["order"], p))
    path_to_id = {p: i + 1 for i, p in enumerate(paths)}

    entity_rows = []
    for p in paths:
        row = nodes[p]
        parts = split_pipe_path(p)
        parent_path = join_pipe_parts(parts[:-1]) if len(parts) > 1 else None

        entity_rows.append({
            "entity_id": path_to_id[p],
            "parent_entity_id": path_to_id.get(parent_path) if parent_path else None,
            "entity_name": row["entity_name"],
            "entity_path": p,
            "raw_names_json": row["raw_names_json"],
            "source_panels_json": row["source_panels_json"],
            "alms": row["alms"],
            "registers": row["registers"],
            "memory_bits": row["memory_bits"],
            "metrics_json": row["metrics_json"],
        })

    return entity_rows


def find_defines_svh(project_dir: pathlib.Path):
    direct = project_dir / "inc" / "defines.svh"
    if direct.exists():
        return direct

    matches = list(project_dir.rglob("defines.svh"))
    for p in matches:
        if len(p.parts) >= 2 and p.parts[-2:] == ("inc", "defines.svh"):
            return p
    return matches[0] if matches else None


def parse_define_value(raw: str):
    if raw is None:
        return None

    v = raw.strip()
    if not v:
        return None

    if "//" in v:
        v = v.split("//", 1)[0].strip()

    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        return v[1:-1]

    if re.fullmatch(r"-?\d+", v):
        return int(v)

    return v


def parse_defines_svh(defines_path: pathlib.Path):
    defines = {}
    if not defines_path or not defines_path.exists():
        return defines

    rx = re.compile(r"^\s*`define\s+([A-Za-z_][A-Za-z0-9_]*)\s*(.*)$")
    with open(defines_path, "r", encoding="utf-8") as f:
        for line in f:
            m = rx.match(line)
            if not m:
                continue
            name = m.group(1)
            raw_value = m.group(2).strip()
            defines[name] = parse_define_value(raw_value)
    return defines


def build_task_parameters(defines: dict):
    """
    Return all defines plus derived parameters.
    No filtering – the upsert function will decide which ones to store.
    """
    params = dict(defines)  # start with all defines

    # Derived parameters (only if base defines exist)
    if "MAX_ROUTERS_X" in defines and "MAX_ROUTERS_Y" in defines:
        routers_x = int(defines["MAX_ROUTERS_X"])
        routers_y = int(defines["MAX_ROUTERS_Y"])
        params["ROUTERS_COUNT"] = routers_x * routers_y
        params["CORE_COUNT"] = routers_x * routers_y

    if "AXI_ID_W_WIDTH" in defines and "AXI_ID_R_WIDTH" in defines:
        params["AXI_MAX_ID_WIDTH"] = max(int(defines["AXI_ID_W_WIDTH"]), int(defines["AXI_ID_R_WIDTH"]))

    if "AXI_DATA_WIDTH" in defines:
        width = int(defines["AXI_DATA_WIDTH"])
        params["AXI_DATA_BYTES"] = width // 8 + (1 if width % 8 != 0 else 0)

    return params


def upsert_task_parameters(pg_dsn: str, task_id: int, params: dict, defines: dict):
    # params keys are define names like "AXI_DATA_WIDTH"
    columns = ["task_id", "defines_json"]
    values = [task_id, json.dumps(defines)]

    for define_key, value in params.items():
        # Convert to lowercase for the column name
        col_name = define_key.lower()
        columns.append(col_name)
        values.append(value)

    placeholders = ["%s"] * len(values)
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in columns if col != "task_id")

    sql = f"""
        INSERT INTO quartus_task_parameters ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
        ON CONFLICT (task_id) DO UPDATE SET
            {set_clause}
    """
    ...


def upsert_postgres(pg_dsn: str, task_id: int, project_name: str, qar_filename: str, reports_json, fmax_json):
    normalized_fmax = normalize_fmax(fmax_json)
    mhz_values = [x["fmax_mhz"] for x in normalized_fmax if x.get("fmax_mhz") is not None]
    top_fmax = max(mhz_values) if mhz_values else None

    entity_rows = rebuild_entities_hybrid(reports_json.get("entities", []))

    summary_json = {
        "report_panels": reports_json.get("matched_panels", []),
        "raw_entity_row_count": len(reports_json.get("entities", [])),
        "normalized_entity_count": len(entity_rows),
    }

    with psycopg.connect(pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO quartus_task_results
                    (task_id, project_name, revision_name, qar_filename, status,
                     compiled_at, top_fmax_mhz, raw_fmax_json, raw_summary_json, updated_at)
                VALUES
                    (%s, %s, %s, %s, %s,
                     now(), %s, %s::jsonb, %s::jsonb, now())
                ON CONFLICT (task_id) DO UPDATE SET
                    project_name = EXCLUDED.project_name,
                    revision_name = EXCLUDED.revision_name,
                    qar_filename = EXCLUDED.qar_filename,
                    status = EXCLUDED.status,
                    compiled_at = EXCLUDED.compiled_at,
                    top_fmax_mhz = EXCLUDED.top_fmax_mhz,
                    raw_fmax_json = EXCLUDED.raw_fmax_json,
                    raw_summary_json = EXCLUDED.raw_summary_json,
                    updated_at = now()
            """, (
                task_id,
                project_name,
                project_name,
                qar_filename,
                "processed",
                top_fmax,
                json.dumps(normalized_fmax),
                json.dumps(summary_json),
            ))

            cur.execute("DELETE FROM quartus_entity_utilization WHERE task_id = %s", (task_id,))

            for row in entity_rows:
                cur.execute("""
                    INSERT INTO quartus_entity_utilization
                        (task_id, entity_id, parent_entity_id, entity_name, entity_path,
                         raw_names_json, source_panels_json, alms, registers, memory_bits, metrics_json)
                    VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (task_id, entity_id) DO UPDATE SET
                        parent_entity_id = EXCLUDED.parent_entity_id,
                        entity_name = EXCLUDED.entity_name,
                        entity_path = EXCLUDED.entity_path,
                        raw_names_json = EXCLUDED.raw_names_json,
                        source_panels_json = EXCLUDED.source_panels_json,
                        alms = EXCLUDED.alms,
                        registers = EXCLUDED.registers,
                        memory_bits = EXCLUDED.memory_bits,
                        metrics_json = EXCLUDED.metrics_json
                """, (
                    task_id,
                    row["entity_id"],
                    row["parent_entity_id"],
                    row["entity_name"],
                    row["entity_path"],
                    json.dumps(row["raw_names_json"]),
                    json.dumps(row["source_panels_json"]),
                    row["alms"],
                    row["registers"],
                    row["memory_bits"],
                    json.dumps(row["metrics_json"]),
                ))
        conn.commit()


def load_json_file(path: pathlib.Path, label: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"ERROR: invalid {label} JSON at {path}: {e}", file=sys.stderr, flush=True)
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                print(f"{i:4d}: {line.rstrip()}", file=sys.stderr, flush=True)
        raise


def main():
    ap = argparse.ArgumentParser(description="Fetch one mesh_<task_id>.qar from GridFS, restore it, extract reports/Fmax, and commit to PostgreSQL.")
    ap.add_argument("--task-id", type=int, required=True, help="Task ID, used to fetch mesh_<task_id>.qar from GridFS")
    ap.add_argument("--mongo-uri", required=True, help="MongoDB URI with access to GridFS")
    ap.add_argument("--mongo-db", default="projects")
    ap.add_argument("--gridfs-bucket", default="artifacts")
    ap.add_argument("--pg-dsn", required=True, help="PostgreSQL DSN")
    ap.add_argument("--quartus-sh", required=True)
    ap.add_argument("--quartus-sta", required=True)
    ap.add_argument("--extract-reports-tcl", default="/opt/distributed_computing/mesh_parser/extract_reports.tcl")
    ap.add_argument("--extract-fmax-tcl", default="/opt/distributed_computing/mesh_parser/extract_fmax.tcl")
    ap.add_argument("--work-root", default="/tmp/quartus_pipeline")
    ap.add_argument("--keep-workdir", action="store_true")
    args = ap.parse_args()

    task_id = args.task_id
    workdir = pathlib.Path(args.work_root) / f"task_{task_id}"
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    qar_path = workdir / f"mesh_{task_id}.qar"
    restore_dir = workdir / "restored"
    reports_json_path = workdir / "reports.json"
    fmax_json_path = workdir / "fmax.json"

    try:
        qar_filename = download_qar(args.mongo_uri, args.mongo_db, args.gridfs_bucket, task_id, qar_path)
        restore_qar(args.quartus_sh, qar_path, restore_dir)

        qpf = find_project_qpf(restore_dir)
        project_dir = qpf.parent
        project_name = parse_project_name(qpf)

        defines_path = find_defines_svh(project_dir)
        defines = parse_defines_svh(defines_path) if defines_path else {}
        task_params = build_task_parameters(defines)

        run_report_extract(args.quartus_sh, args.extract_reports_tcl, project_dir, project_name, reports_json_path)
        run_fmax_extract(args.quartus_sta, args.extract_fmax_tcl, project_dir, project_name, fmax_json_path)

        reports_json = load_json_file(reports_json_path, "reports")
        fmax_json = load_json_file(fmax_json_path, "fmax")

        upsert_postgres(args.pg_dsn, task_id, project_name, qar_filename, reports_json, fmax_json)
        upsert_task_parameters(args.pg_dsn, task_id, task_params, defines)

        print(f"SUCCESS: processed task_id={task_id}", flush=True)
    except Exception as e:
        print(f"ERROR: task_id={task_id}: {e}", file=sys.stderr, flush=True)
        raise
    finally:
        if args.keep_workdir:
            print(f"Kept workdir: {workdir}", flush=True)
        else:
            shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()