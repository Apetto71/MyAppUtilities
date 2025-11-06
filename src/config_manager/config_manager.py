import os
import threading
import configparser
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class ConfigReloader(FileSystemEventHandler):
    """Gestisce l'evento di modifica del file di configurazione .ini."""

    def __init__(self, config_manager):
        self.config_manager = config_manager

    def on_modified(self, event):
        if event.src_path == os.path.abspath(self.config_manager.config_path):
            print(f"[ConfigReloader] File modificato: {event.src_path}")
            self.config_manager._reload()


class ConfigManager:
    """
    Singleton per gestire caricamento e aggiornamento automatico della configurazione .ini.
    """

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, config_path: str):
        """
        Implementazione thread-safe del pattern Singleton.
        """
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super(ConfigManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: str):
        if getattr(self, "_initialized", False):
            # Evita di rieseguire l'inizializzazione per il Singleton
            return

        self.config_path = os.path.abspath(config_path)
        self._config = configparser.ConfigParser()
        self._lock = threading.Lock()
        self._observer = None

        self._load()
        self._start_watching()

        self._initialized = True  # Segnala che l'istanza è già inizializzata
        print(f"[ConfigManager] Singleton inizializzato con {self.config_path}")

    def _load(self):
        """Carica il file di configurazione .ini."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"File di configurazione non trovato: {self.config_path}")

        with self._lock:
            self._config.read(self.config_path)
        print("[ConfigManager] Configurazione caricata da file.")

    def _reload(self):
        """Ricarica la configurazione se il file viene modificato."""
        try:
            self._load()
            print("[ConfigManager] Configurazione aggiornata automaticamente.")
        except Exception as e:
            print(f"[ConfigManager] Errore durante il reload: {e}")

    def _start_watching(self):
        """Avvia il monitoraggio del file di configurazione."""
        event_handler = ConfigReloader(self)
        observer = Observer()
        observer.schedule(event_handler, os.path.dirname(self.config_path), recursive=False)
        observer.start()
        self._observer = observer
        print(f"[ConfigManager] Monitoraggio avviato su {self.config_path}")

    # --- Metodi pubblici di accesso ai parametri ---

    def get(self, section: str, option: str, fallback=None):
        with self._lock:
            return self._config.get(section, option, fallback=fallback)

    def get_int(self, section: str, option: str, fallback=None):
        with self._lock:
            return self._config.getint(section, option, fallback=fallback)

    def get_bool(self, section: str, option: str, fallback=None):
        with self._lock:
            return self._config.getboolean(section, option, fallback=fallback)

    def get_float(self, section: str, option: str, fallback=None):
        with self._lock:
            return self._config.getfloat(section, option, fallback=fallback)

    def sections(self):
        with self._lock:
            return self._config.sections()

    def stop(self):
        """Ferma il monitoraggio del file."""
        if self._observer:
            self._observer.stop()
            self._observer.join()
            print("[ConfigManager] Monitoraggio interrotto.")
