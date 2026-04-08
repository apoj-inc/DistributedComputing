#!/bin/bash

# ----------------------------------------------------------------------
# generate_mesh_dc_tasks.sh
#
# Generate full-factorial combinations from a parameter file and submit
# one dc_cli task per combination.
#
# Parameter file format (same as generate_mesh_tasks.sh):
#   # comments are ignored
#   PARAM_NAME  min max
#   PARAM_NAME  min-max
#
# Example:
#   AXI_ID_WIDTH 1 3
#   BAUD_RATE 1-2
#
# Default task template:
#   dc_cli --base-url <url> tasks add --cmd powershell ...
# ----------------------------------------------------------------------

set -euo pipefail

usage() {
    cat <<'USAGE'
Usage:
  generate_mesh_dc_tasks.sh <parameter_file> [options]

Options:
  --base-url <url>        dc_cli base URL (default: http://10.8.0.15:8080)
  --task-prefix <prefix>  Task name prefix passed to wrapper (default: test_)
  --start-id <n>          Starting numeric suffix for generated task name (default: 1)
  --project-dir <path>    Remote project dir (default: C:/HDLNoCGEN/cosim_top)
  --output-dir <path>     Remote output dir (default: C:/HDLNoCGEN/db)
  --wrapper <path>        Wrapper ps1 path (default: C:/HDLNoCGEN/quartus_agent_wrapper.ps1)
  --project <name>        Quartus project file (default: cosim_top)
  --tcl <file.tcl>        Quartus tcl script (default: compile_with_defines.tcl)
  --dc-cli <bin>          dc_cli executable (default: dc_cli)
  --powershell <bin>      PowerShell executable for agent (default: powershell)
  --submit-delay <sec>    Delay between task submissions in seconds (default: 0.3)
  --dry-run               Print commands only, do not execute
  -h, --help              Show this help
USAGE
}

if [[ $# -lt 1 ]]; then
    usage >&2
    exit 1
fi

parameter_file="$1"
shift

if [[ ! -f "$parameter_file" ]]; then
    echo "Error: parameter file '$parameter_file' not found." >&2
    exit 1
fi

base_url="http://127.0.0.1:8080"
task_prefix="task_"
start_id=1
project_dir="C:/HDLNoCGEN/cosim_top"
output_dir="C:/HDLNoCGEN/db"
wrapper_path="C:/HDLNoCGEN/quartus_agent_wrapper.ps1"
quartus_project="cosim_top"
quartus_tcl="compile_with_defines.tcl"
dc_cli_bin="/pg/HDLNoCGen_s/src/DistributedComputing/build/debug/src/cli/dc_cli"
powershell_bin="powershell"
submit_delay="0.001"
dry_run=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --base-url)
            base_url="$2"; shift 2 ;;
        --task-prefix)
            task_prefix="$2"; shift 2 ;;
        --start-id)
            start_id="$2"; shift 2 ;;
        --project-dir)
            project_dir="$2"; shift 2 ;;
        --output-dir)
            output_dir="$2"; shift 2 ;;
        --wrapper)
            wrapper_path="$2"; shift 2 ;;
        --project)
            quartus_project="$2"; shift 2 ;;
        --tcl)
            quartus_tcl="$2"; shift 2 ;;
        --dc-cli)
            dc_cli_bin="$2"; shift 2 ;;
        --powershell)
            powershell_bin="$2"; shift 2 ;;
        --submit-delay)
            submit_delay="$2"; shift 2 ;;
        --dry-run)
            dry_run=1; shift ;;
        -h|--help)
            usage
            exit 0 ;;
        *)
            echo "Error: unknown option '$1'" >&2
            usage >&2
            exit 1 ;;
    esac
done

if ! [[ "$start_id" =~ ^[0-9]+$ ]]; then
    echo "Error: --start-id must be a non-negative integer." >&2
    exit 1
fi
if ! [[ "$submit_delay" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    echo "Error: --submit-delay must be a non-negative number." >&2
    exit 1
fi

# Read parameters into arrays
names=()
ranges=()
while read -r line; do
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

    name=$(echo "$line" | awk '{print $1}')
    spec=$(echo "$line" | awk '{$1=""; print $0}' | sed 's/^[[:space:]]*//')

    if [[ "$spec" =~ ^([0-9]+)[[:space:]]+([0-9]+)$ ]]; then
        min="${BASH_REMATCH[1]}"
        max="${BASH_REMATCH[2]}"
    elif [[ "$spec" =~ ^([0-9]+)-([0-9]+)$ ]]; then
        min="${BASH_REMATCH[1]}"
        max="${BASH_REMATCH[2]}"
    else
        echo "Warning: skipping invalid line: $line" >&2
        continue
    fi

    if (( min > max )); then
        tmp=$min; min=$max; max=$tmp
    fi

    names+=("$name")
    ranges+=("$min $max")
done < "$parameter_file"

if [[ ${#names[@]} -eq 0 ]]; then
    echo "Error: no valid parameters found in '$parameter_file'." >&2
    exit 1
fi

# Precompute value lists
values=()
total_combinations=1
for idx in "${!names[@]}"; do
    read -r min max <<< "${ranges[$idx]}"
    seq_list=($(seq "$min" "$max"))
    values[$idx]="${seq_list[*]}"
    total_combinations=$((total_combinations * ${#seq_list[@]}))
done

echo "# Total combinations: $total_combinations" >&2
echo "# base_url=$base_url, task_prefix=$task_prefix, start_id=$start_id, submit_delay=${submit_delay}s" >&2

submitted=0

submit_task() {
    local task_id="$1"
    shift
    local -a defs=("$@")

    local quartus_args_line="-t $quartus_tcl --project $quartus_project"
    local def
    for def in "${defs[@]}"; do
        quartus_args_line+=" --define $def"
    done

    local -a cmd=(
        "$dc_cli_bin" --base-url "$base_url" tasks add
        --timeout 1200
	--cmd "$powershell_bin"
        --arg=-NoLogo
        --arg=-NoProfile
        --arg=-NonInteractive
        --arg=-ExecutionPolicy
        --arg=Bypass
        --arg=-File
        --arg="$wrapper_path"
        --arg=-Id
        --arg="$task_id"
        --arg=-ProjectDir
        --arg="$project_dir"
        --arg=-OutputDir
        --arg="$output_dir"
        --arg=-QuartusArgsLine
        --arg="$quartus_args_line"
    )

    if (( dry_run == 1 )); then
        printf '%q ' "${cmd[@]}"
        printf '\n'
    else
        "${cmd[@]}"
        local next_count=$((submitted + 1))
        if (( next_count < total_combinations )); then
            sleep "$submit_delay"
        fi
    fi

    submitted=$((submitted + 1))
}

generate() {
    local depth="$1"
    shift
    local -a prefix_defs=("$@")

    if [[ "$depth" -eq "${#names[@]}" ]]; then
        local task_id="${task_prefix}${task_num}"
        submit_task "$task_id" "${prefix_defs[@]}"
        task_num=$((task_num + 1))
        return
    fi

    local -a this_values=(${values[$depth]})
    local name="${names[$depth]}"
    local val
    for val in "${this_values[@]}"; do
        generate $((depth + 1)) "${prefix_defs[@]}" "$name=$val"
    done
}

task_num="$start_id"
generate 0

echo "# Done. Submitted: $submitted task(s)." >&2
