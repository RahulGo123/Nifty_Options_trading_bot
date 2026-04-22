import multiprocessing
import numpy as np
from datetime import datetime
from loguru import logger
from analytics.greeks_engine import GreeksEngine
from config.constants import DEFAULT_RISK_FREE_RATE


def run_worker(
    input_queue: multiprocessing.Queue,
    result_queue: multiprocessing.Queue,
    stop_event: multiprocessing.Event,
):
    """
    Entry point for the Greeks worker process.

    Runs in a completely separate OS process on its own CPU core.
    Receives batched tick data from the main process every second,
    computes Greeks and IV for all strikes, sends results back.

    The main process never waits for these results — it reads
    the latest available result the next time the signal engine
    runs. If the worker is slow, the signal engine uses the
    previous result with a staleness flag.

    Communication:
        Main → Worker: input_queue  (batch dict)
        Worker → Main: result_queue (dict of numpy arrays)

    Risk-free rate:
    Passed inside every batch from the main process.
    This ensures the worker always uses the most recently
    updated rate without any shared memory or IPC complexity.
    Falls back to DEFAULT_RISK_FREE_RATE if not in batch.
    """
    logger.info("[WORKER PROCESS] Greeks worker started.")
    engine = GreeksEngine()

    while not stop_event.is_set():
        try:
            try:
                batch = input_queue.get(timeout=1.0)
            except Exception:
                continue

            if batch is None:
                logger.info("[WORKER PROCESS] Shutdown signal received.")
                break

            result = _process_batch(engine, batch)

            # Discard oldest result if queue is full
            if result_queue.full():
                try:
                    result_queue.get_nowait()
                except Exception:
                    pass

            result_queue.put_nowait(result)

        except Exception as e:
            logger.error(f"[WORKER PROCESS] Error processing batch: {e}")

    logger.info("[WORKER PROCESS] Greeks worker stopped.")


def _process_batch(engine: GreeksEngine, batch: dict) -> dict:
    """
    Processes a single batch of tick data.

    Only the most recent tick per strike is used.
    Older ticks within the same 1-second window are discarded.

    batch structure:
    {
        'spot':             float,
        'time_to_expiry':   float,      # in years (days / 365)
        'risk_free_rate':   float,      # from constants — updated weekly
        'ticks': {
            strike_int: {
                'CE': {'mid_price': float},
                'PE': {'mid_price': float},
            }
        }
    }
    """
    spot = batch.get("spot", 0.0)
    time_to_expiry = batch.get("time_to_expiry", 0.0)
    risk_free_rate = batch.get("risk_free_rate", DEFAULT_RISK_FREE_RATE)
    ticks = batch.get("ticks", {})

    if not ticks or spot <= 0 or time_to_expiry <= 0:
        return {}

    strikes_list = []
    premiums_list = []
    types_list = []

    for strike, pair in sorted(ticks.items()):
        for option_type in ["CE", "PE"]:
            if option_type in pair:
                mid_price = pair[option_type].get("mid_price", 0.0)
                if mid_price > 0:
                    strikes_list.append(float(strike))
                    premiums_list.append(mid_price)
                    types_list.append(option_type)

    if not strikes_list:
        return {}

    strikes = np.array(strikes_list)
    premiums = np.array(premiums_list)
    types = np.array(types_list)

    result = engine.compute_batch(
        spot=spot,
        strikes=strikes,
        premiums=premiums,
        option_types=types,
        time_to_expiry=time_to_expiry,
        risk_free_rate=risk_free_rate,
    )

    result["timestamp"] = datetime.now()
    result["spot"] = spot
    result["time_to_expiry"] = time_to_expiry

    return result


class WorkerProcessManager:
    """Manages the OS-level process and queues for the Greeks Engine."""

    def __init__(self):
        self.process = None
        self.input_queue = None
        self.result_queue = None
        self.stop_event = None

    def start(self):
        if self.process is not None and self.process.is_alive():
            return

        self.input_queue = multiprocessing.Queue(maxsize=10)
        self.result_queue = multiprocessing.Queue(maxsize=10)
        self.stop_event = multiprocessing.Event()

        self.process = multiprocessing.Process(
            target=run_worker,
            args=(self.input_queue, self.result_queue, self.stop_event),
            daemon=True,
            name="GreeksWorkerProcess",
        )
        self.process.start()

    def stop(self):
        if self.stop_event:
            self.stop_event.set()
        if self.process:
            self.process.join(timeout=2.0)
            if self.process.is_alive():
                self.process.terminate()


# The singleton instance imported by market_scheduler and other modules
worker_process = WorkerProcessManager()
