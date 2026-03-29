import psycopg2
import time
import requests
import subprocess
import os
import signal
import random
import string

class DbHelper:
    def __init__(self, host, port, user, password, dbname, sslmode):
        self.conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname, sslmode=sslmode
        )
        self.conn.autocommit = False

    def clear_all(self):
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE task_assignments, tasks, agents RESTART IDENTITY CASCADE")
        self.conn.commit()

    def has_agent(self, agent_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM agents WHERE agent_id = %s", (agent_id,))
            return cur.fetchone() is not None

    def get_agent_status(self, agent_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT status::text FROM agents WHERE agent_id = %s", (agent_id,))
            row = cur.fetchone()
            return row[0].lower() if row else ""

    def get_task_state(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT state::text FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0].lower() if row else ""

    def get_task_assigned_agent(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT assigned_agent FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else None

    def task_started(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT started_at IS NOT NULL FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else False

    def task_finished(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT finished_at IS NOT NULL FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else False

    def get_task_exit_code(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT exit_code FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else None

    def get_task_error_message(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT error_message FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else ""

    def count_assignments(self, task_id, only_open=False):
        with self.conn.cursor() as cur:
            query = "SELECT COUNT(*) FROM task_assignments WHERE task_id = %s"
            if only_open:
                query += " AND unassigned_at IS NULL"
            cur.execute(query, (task_id,))
            return cur.fetchone()[0]

    def get_constraint_os(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT constraints->>'os' FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else None

    def get_constraint_cpu(self, task_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT (constraints->>'cpu_cores')::int FROM tasks WHERE task_id = %s", (task_id,))
            row = cur.fetchone()
            return row[0] if row else None

class MasterProcess:
    def __init__(self, binary, env, work_dir="."):
        self.binary = binary
        self.env = env
        self.work_dir = work_dir
        self.process = None

    def start(self):
        env = os.environ.copy()
        env.update(self.env)
        self.process = subprocess.Popen(
            [self.binary],
            cwd=self.work_dir,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            preexec_fn=os.setsid
        )

    def stop(self):
        if self.process and self.process.poll() is None:
            try:
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait()
        self.process = None

def wait_for_master_ready(host, port, timeout=10):
    url = f"http://{host}:{port}/api/v1/agents?limit=1"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=1)
            if r.status_code == 200:
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.1)
    return False

def extract_numeric_token(text):
    import re
    m = re.search(r'\d+', text)
    return m.group(0) if m else None

def make_id(prefix):
    return f"{prefix}_{os.getpid()}_{int(time.time()*1000)}_{random.randint(0, 100000)}"
