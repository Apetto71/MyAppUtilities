# import threading
# import time
# import logging
# from typing import Callable, Generic, List, TypeVar
#
# T = TypeVar("T")
#
# logger = logging.getLogger(__name__)  # <--- Logger per questo modulo
#
# class BufferedWorker(Generic[T]):
#     def __init__(
#         self,
#         flush_callback: Callable[[List[T]], None],
#         max_size: int = 500,
#         flush_interval: float = 5.0,
#     ):
#         """
#         BufferedWorker accumula oggetti e li elabora in blocco tramite flush_callback.
#
#         :param flush_callback: Funzione da chiamare per elaborare il batch (es: salva su DB)
#         :param max_size: Numero massimo di elementi prima del flush automatico
#         :param flush_interval: Tempo massimo (in secondi) tra un flush e l'altro
#         """
#         self._flush_callback = flush_callback
#         self._max_size = max_size
#         self._flush_interval = flush_interval
#
#         self._buffer: List[T] = []
#         self._lock = threading.Lock()
#         self._last_flush = time.time()
#         self._running = True
#
#         self._thread = threading.Thread(target=self._flush_daemon, name="BufferedWorkerThread")
#         self._thread.daemon = True
#         self._thread.start()
#
#         logger.debug(f"BufferedWorker initialized (max_size={max_size}, flush_interval={flush_interval}s)")
#
#     def add(self, item: T):
#         with self._lock:
#             self._buffer.append(item)
#             logger.debug(f"Item added to buffer. Current size: {len(self._buffer)}")
#             if len(self._buffer) >= self._max_size:
#                 logger.debug("Buffer size threshold reached. Flushing buffer.")
#                 self._flush_locked()
#
#     def _flush_daemon(self):
#         while self._running:
#             time.sleep(1)
#             with self._lock:
#                 if self._buffer and (time.time() - self._last_flush >= self._flush_interval):
#                     logger.debug("Flush interval reached. Flushing buffer.")
#                     self._flush_locked()
#
#     def _flush_locked(self):
#         if not self._buffer:
#             return
#         data_to_flush = self._buffer
#         self._buffer = []
#         self._last_flush = time.time()
#         logger.debug(f"Flushing {len(data_to_flush)} items from buffer.")
#         try:
#             self._flush_callback(data_to_flush)
#             logger.info(f"Successfully flushed {len(data_to_flush)} items.")
#         except Exception as e:
#             logger.error(f"Error during flush: {e}", exc_info=True)
#             # (facoltativo) reinserire i dati se necessario
#
#     def stop(self):
#         logger.debug("Stopping BufferedWorker...")
#         self._running = False
#         self._thread.join()
#         with self._lock:
#             if self._buffer:
#                 logger.debug("Final flush on stop.")
#             self._flush_locked()
#         logger.info("BufferedWorker stopped.")


# Modifiche al modulo Buffered_worker_Utils.py (BufferedWorker)

import threading
import time
import logging
from typing import Callable, Generic, List, TypeVar

T = TypeVar("T")

logger = logging.getLogger(__name__)  # <--- Logger per questo modulo

class BufferedWorker(Generic[T]):
    def __init__(
            self,
            flush_callback: Callable[[List[T]], None],  # La callback NON DEVE ritornare un valore
            max_size: int = 500,
            flush_interval: float = 5.0,
    ):
        """
        BufferedWorker accumula oggetti e li elabora in blocco tramite flush_callback.

        :param flush_callback: Funzione da chiamare per elaborare il batch (es: salva su DB)
        :param max_size: Numero massimo di elementi prima del flush automatico
        :param flush_interval: Tempo massimo (in secondi) tra un flush e l'altro
        """
        self._flush_callback = flush_callback
        self._max_size = max_size
        self._flush_interval = flush_interval

        self._buffer: List[T] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._running = True

        self._thread = threading.Thread(target=self._flush_daemon, name="BufferedWorkerThread")
        self._thread.daemon = True
        self._thread.start()

        logger.debug(f"BufferedWorker initialized (max_size={max_size}, flush_interval={flush_interval}s)")

    def add(self, item: T):
        with self._lock:
            self._buffer.append(item)
            logger.debug(f"Item added to buffer. Current size: {len(self._buffer)}")
            if len(self._buffer) >= self._max_size:
                logger.debug("Buffer size threshold reached. Flushing buffer.")
                # Chiamiamo _flush_locked() che gestisce il flush in modo sincrono
                self._flush_locked()

    def _flush_daemon(self):
        while self._running:
            time.sleep(1)
            with self._lock:
                if self._buffer and (time.time() - self._last_flush >= self._flush_interval):
                    logger.debug("Flush interval reached. Flushing buffer.")
                    self._flush_locked()

    def _flush_locked(self):
        """
        Esegue il flush, passando i dati alla callback.
        Gestisce la protezione del buffer ma ignora il valore di ritorno della callback.
        """
        if not self._buffer:
            return

        # Il buffer viene svuotato PRIMA della chiamata alla callback.
        # Questo è importante: se la callback fallisce, i dati sono persi
        # in memoria ma il sistema non si blocca e può continuare a bufferizzare nuovi dati.
        # La gestione del recupero dei dati falliti deve avvenire nella callback stessa.
        data_to_flush = self._buffer
        self._buffer = []
        self._last_flush = time.time()
        logger.debug(f"Flushing {len(data_to_flush)} items from buffer.")

        try:
            # Chiamata alla callback: L'intera logica di DB/ACK/NACK è qui
            self._flush_callback(data_to_flush)
            logger.info(f"Successfully flushed {len(data_to_flush)} items.")

        except Exception as e:
            # Errore grave/critico durante l'esecuzione della callback.
            # Questo errore indica che il processo è probabilmente instabile
            # (es. connessione DB critica o errore logico non gestito).
            logger.error(f"Critical error during flush callback: {e}", exc_info=True)
            # NON tentiamo di ri-accodare il buffer qui; è responsabilità della callback
            # gestire il fallback o il logging dei record falliti.

    def stop(self):
        logger.debug("Stopping BufferedWorker...")
        self._running = False
        self._thread.join()
        with self._lock:
            if self._buffer:
                logger.debug("Final flush on stop.")
            self._flush_locked()
        logger.info("BufferedWorker stopped.")