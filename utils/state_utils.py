import os
import pickle
import threading
import time
import traceback

from logger import log

from config.config import STATE_FILE

state_lock = threading.Lock()  # For safe concurrent access


def save_state(state):
    state_dir = os.path.dirname(STATE_FILE)
    if state_dir and not os.path.exists(state_dir):
        os.makedirs(state_dir, exist_ok=True)

    try:
        with state_lock:
            with open(STATE_FILE, 'wb') as f:
                pickle.dump(state, f)
        log.info(f"State saved successfully with {len(state)} files tracked at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        log.critical(f"Failed to save state: {e}\n{traceback.format_exc()}")


def load_state():
    try:
        with open(STATE_FILE, 'rb') as f:
            state = pickle.load(f)
            log.info(f"State loaded with {len(state)} files tracked.")
            return state
    except (FileNotFoundError, EOFError):
        log.info("No valid state file found, starting fresh.")
        save_state({})
        return {}
    except Exception as e:
        log.critical(f"Failed to load state: {e}\n{traceback.format_exc()}")
        save_state({})
        return {}
