import os
import pickle
import threading
import time
import traceback

from logger import log

from config.config import STATE_FILE

state_lock = threading.Lock()  # For safe concurrent access


def save_state(state):
    log.debug(f"Saving state for {len(state)} files")
    state_dir = os.path.dirname(STATE_FILE)
    if state_dir and not os.path.exists(state_dir):
        os.makedirs(state_dir, exist_ok=True)
        log.debug(f"Created state directory {state_dir}")

    try:
        with state_lock:
            with open(STATE_FILE, 'wb') as f:
                pickle.dump(state, f)
        log.debug(f"State written to {STATE_FILE}")
        log.info(f"State saved successfully with {len(state)} files tracked at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception as e:
        log.critical(f"Failed to save state: {e}\n{traceback.format_exc()}")


def load_state():
    try:
        log.debug(f"Loading state from {STATE_FILE}")
        with open(STATE_FILE, 'rb') as f:
            state = pickle.load(f)
            log.info(f"State loaded with {len(state)} files tracked.")
            return state
    except (FileNotFoundError, EOFError):
        log.info("No valid state file found, starting fresh.")
        save_state({})
        log.debug("Initialized new empty state")
        return {}
    except Exception as e:
        log.critical(f"Failed to load state: {e}\n{traceback.format_exc()}")
        save_state({})
        log.debug("Saved empty state after failure to load existing state")
        return {}