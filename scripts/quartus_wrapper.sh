#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------
# Utility functions
# ------------------------------------------------------------
fail() {
    echo "ERROR: $*" >&2
    exit 1
}

check_command() {
    command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

# ------------------------------------------------------------
# Defaults
# ------------------------------------------------------------
QUARTUS_SH="quartus_sh"
MONGO_URI=""
MONGO_DB=""
ID=""
PROJECT_DIR=""
OUTPUT_DIR=""
QUARTUS_ARGS_LINE=""

# ------------------------------------------------------------
# Parse command line arguments
# ------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --id)
            ID="$2"
            shift 2
            ;;
        --project-dir)
            PROJECT_DIR="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --quartus-args-line)
            QUARTUS_ARGS_LINE="$2"
            shift 2
            ;;
        --mongo-uri)
            MONGO_URI="$2"
            shift 2
            ;;
        --mongo-db)
            MONGO_DB="$2"
            shift 2
            ;;
        --quartus-sh)
            QUARTUS_SH="$2"
            shift 2
            ;;
        *)
            fail "Unknown option: $1"
            ;;
    esac
done

# Validate required arguments
[[ -z "$ID" ]] && fail "Missing --id"
[[ -z "$PROJECT_DIR" ]] && fail "Missing --project-dir"
[[ -z "$OUTPUT_DIR" ]] && fail "Missing --output-dir"
[[ -z "$QUARTUS_ARGS_LINE" ]] && fail "Missing --quartus-args-line"
[[ -z "$MONGO_URI" ]] && fail "Missing --mongo-uri"
[[ -z "$MONGO_DB" ]] && fail "Missing --mongo-db"

# ------------------------------------------------------------
# Check required tools
# ------------------------------------------------------------
check_command "$QUARTUS_SH"
check_command "mongofiles"
# Prefer mongosh over legacy mongo
if command -v mongosh >/dev/null 2>&1; then
    MONGO_CMD="mongosh"
else
    command -v mongo >/dev/null 2>&1 || fail "Neither mongosh nor mongo found"
    MONGO_CMD="mongo"
fi

# ------------------------------------------------------------
# Prepare directories
# ------------------------------------------------------------
if [[ ! -d "$PROJECT_DIR" ]]; then
    fail "Project directory not found: $PROJECT_DIR"
fi

if [[ ! -d "$OUTPUT_DIR" ]]; then
    mkdir -p "$OUTPUT_DIR" || fail "Cannot create output directory: $OUTPUT_DIR"
fi

# ------------------------------------------------------------
# Run Quartus
# ------------------------------------------------------------
cd "$PROJECT_DIR" || fail "Cannot cd into $PROJECT_DIR"
echo "Running: $QUARTUS_SH $QUARTUS_ARGS_LINE"
echo "Working directory: $PROJECT_DIR"

# Use eval to properly handle quoted arguments inside QUARTUS_ARGS_LINE
eval "$QUARTUS_SH $QUARTUS_ARGS_LINE"
QUARTUS_EXIT=$?
if [[ $QUARTUS_EXIT -ne 0 ]]; then
    fail "quartus_sh finished with non-zero exit code: $QUARTUS_EXIT"
fi
cd - >/dev/null

# ------------------------------------------------------------
# Locate the most recent .qar file
# ------------------------------------------------------------
mapfile -t qar_files < <(
    find "$PROJECT_DIR" -type f -name "*.qar" -printf "%T@ %p\0" \
    | sort -z -n -r \
    | head -z -n 1 \
    | cut -z -d ' ' -f2-
)
if [[ ${#qar_files[@]} -eq 0 ]]; then
    fail "No .qar file found in project directory: $PROJECT_DIR"
fi

qar_file="${qar_files[0]}"
# Count total number of .qar files
total_qar=$(find "$PROJECT_DIR" -type f -name "*.qar" | wc -l)
if [[ $total_qar -gt 1 ]]; then
    echo "Warning: Found multiple .qar files, using the most recent: $qar_file" >&2
fi

echo "Archive selected: $qar_file"

# ------------------------------------------------------------
# Upload to MongoDB GridFS
# ------------------------------------------------------------
GRIDFS_BUCKET="artifacts"
GRIDFS_FILENAME="${ID}.qar"

echo "Uploading artifact to GridFS bucket '$GRIDFS_BUCKET' as '$GRIDFS_FILENAME' to '$MONGO_DB'"

# Use mongofiles to upload the file
mongofiles put "$GRIDFS_FILENAME" \
    --uri "$MONGO_URI" \
    --db "$MONGO_DB" \
    --prefix "$GRIDFS_BUCKET" \
    -r \
    --local "$qar_file" \
    || fail "mongofiles upload failed"

# Add metadata.taskId to the uploaded file (the most recent with this filename)
$MONGO_CMD "$MONGO_URI/$MONGO_DB" --quiet --eval "
    var fn = '$GRIDFS_FILENAME';
    var taskId = '$ID';
    var coll = db.getCollection('$GRIDFS_BUCKET.files');
    var doc = coll.findOne({filename: fn}, {sort: {uploadDate: -1}});
    if (doc) {
        coll.updateOne({_id: doc._id}, {\$set: {'metadata.taskId': taskId}});
        print('Added metadata.taskId=' + taskId + ' to file ' + fn);
    } else {
        throw 'No file found with filename ' + fn;
    }
" || fail "Failed to add metadata to GridFS file"

# ------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------
rm -f "$qar_file" || fail "Failed to remove local .qar file: $qar_file"

echo "GridFS upload completed for task: $ID"
exit 0
