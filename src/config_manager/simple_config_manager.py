import os
import threading
import configparser


class SimpleConfigManager:
    """
    Singleton per gestire il caricamento della configurazione .ini e variabili d'ambiente.
    Senza monitoraggio attivo (no hot-reloading).
    """

    _instance = None
    _instance_lock = threading.Lock()

    def __new__(cls, config_path: str):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super(SimpleConfigManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: str):
        if getattr(self, "_initialized", False):
            return

        self.config_path = os.path.abspath(config_path)
        self._config = configparser.ConfigParser()
        self._lock = threading.Lock()

        self._load()
        self._initialized = True
        print(f"[ConfigManager] Inizializzato (Statico) con {self.config_path}")

    def _load(self):
        """Carica il file di configurazione una sola volta all'avvio."""
        if os.path.exists(self.config_path):
            try:
                with self._lock:
                    self._config.read(self.config_path)
                print("[ConfigManager] Configurazione caricata correttamente.")
            except Exception as e:
                print(f"[ConfigManager] Errore critico nel caricamento del file: {e}")
        else:
            print(f"[ConfigManager] AVVISO: File {self.config_path} non trovato. Uso solo Env Vars.")

    def get(self, section: str, option: str, fallback=None):
        # 1. Priorità Variabile d'Ambiente (SECTION_OPTION)
        env_key = f"{section}_{option}".upper()
        env_value = os.getenv(env_key)
        if env_value and env_value.strip():
            return env_value.strip()

        # 2. Fallback su file .ini
        with self._lock:
            return self._config.get(section, option, fallback=fallback)

    def get_string(self, section: str, option: str, fallback=None):
        """
        Recupera il valore stringa. Alias di get() per coerenza con get_int/get_float.
        """
        return self.get(section, option, fallback=fallback)

    def get_int(self, section: str, option: str, fallback=None):
        env_key = f"{section}_{option}".upper()
        env_value = os.getenv(env_key)
        if env_value and env_value.strip():
            try:
                return int(env_value.strip())
            except ValueError:
                pass

        with self._lock:
            return self._config.getint(section, option, fallback=fallback)

    def get_float(self, section: str, option: str, fallback=None):
        env_key = f"{section}_{option}".upper()
        env_value = os.getenv(env_key)
        if env_value and env_value.strip():
            try:
                return float(env_value.strip())
            except ValueError:
                pass

        with self._lock:
            return self._config.getfloat(section, option, fallback=fallback)

    def get_bool(self, section: str, option: str, fallback=None):
        env_key = f"{section}_{option}".upper()
        env_value = os.getenv(env_key).strip().lower() if os.getenv(env_key) else None

        if env_value in ("1", "true", "yes", "on"): return True
        if env_value in ("0", "false", "no", "off"): return False

        with self._lock:
            return self._config.getboolean(section, option, fallback=fallback)