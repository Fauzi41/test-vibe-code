import datetime
import logging

logging.basicConfig(level=logging.INFO)

users = {
    'alice': {'quota': 3, 'executed': 0},
    'bob':   {'quota': 5, 'executed': 0},
}

tasks = [
    {'user': 'alice', 'time': '12:00', 'action': 'sync',   'target': '/data/x', 'done': False},
    {'user': 'bob',   'time': '12:00', 'action': 'backup', 'target': '/srv/y',  'done': False},
    {'user': 'alice', 'time': '12:00', 'action': 'delete', 'target': '/tmp/z',  'done': False},
]

ALLOWED_ACTIONS = {'sync', 'backup', 'delete'}

def execute(task):
    # ganti dengan implementasi nyata
    print(f"Executing {task['action']} on {task['target']} for {task['user']}")

def run(now=None):
    now = now or datetime.datetime.now().strftime('%H:%M')
    for task in tasks:
        if task['done'] or task['time'] > now:   # sudah selesai / belum jatuh tempo
            continue

        user = users.get(task['user'])
        if user is None:
            logging.warning("Unknown user: %s", task['user'])
            task['done'] = True
            continue

        if task['action'] not in ALLOWED_ACTIONS:
            logging.warning("Action not allowed: %s", task['action'])
            task['done'] = True
            continue

        if user['executed'] >= user['quota']:
            logging.info("%s has exceeded quota.", task['user'])
            continue  # tetap pending, dicoba lagi saat kuota di-reset

        try:
            execute(task)
            user['executed'] += 1
        except Exception:
            logging.exception("Task failed: %s", task)
        finally:
            task['done'] = True

if __name__ == "__main__":
    run()