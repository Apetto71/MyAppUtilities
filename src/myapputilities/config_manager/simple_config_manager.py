import os
import threading
import configparser


class SimpleConfigManager:
    """
    Singleton per gestire il caricamento della configurazione .ini e variabili d'ambiente.
    Supporta la priorità delle Env Vars per l'integrazione Docker.
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
        # Normalizziamo il percorso
        new_path = os.path.abspath(config_path)

        # Se siamo già inizializzati E il percorso è lo stesso, non facciamo nulla
        if getattr(self, "_initialized", False) and self.config_path == new_path:
            return

        self.config_path = new_path
        self._config = configparser.ConfigParser()
        self._lock = threading.Lock()

        self._load()
        self._initialized = True
        print(f"[ConfigManager] Inizializzato/Aggiornato con {self.config_path}")

    def _load(self):
        """Carica il file di configurazione una sola volta all'avvio."""
        if os.path.exists(self.config_path):
            try:
                with self._lock:
                    self._config.read(self.config_path)
            except Exception as e:
                print(f"[ConfigManager] Errore critico nel caricamento del file: {e}")
        else:
            print(f"[ConfigManager] AVVISO: File {self.config_path} non trovato. Uso solo Env Vars.")

    def _get_env_key(self, section: str, option: str):
        """Standardizza la chiave per le variabili d'ambiente."""
        return f"{section}_{option}".upper()

    def get(self, section: str, option: str, fallback=None):
        # 1. Priorità Variabile d'Ambiente (SECTION_OPTION)
        env_key = self._get_env_key(section, option)
        env_value = os.getenv(env_key)
        if env_value is not None and env_value.strip():
            return env_value.strip()

        # 2. Fallback su file .ini
        with self._lock:
            return self._config.get(section, option, fallback=fallback)

    def get_string(self, section: str, option: str, fallback=None):
        """Alias di get() per coerenza."""
        return self.get(section, option, fallback=fallback)

    def get_int(self, section: str, option: str, fallback=None):
        env_key = self._get_env_key(section, option)
        env_value = os.getenv(env_key)
        if env_value is not None and env_value.strip():
            try:
                return int(env_value.strip())
            except ValueError:
                pass

        with self._lock:
            return self._config.getint(section, option, fallback=fallback)

    def get_float(self, section: str, option: str, fallback=None):
        env_key = self._get_env_key(section, option)
        env_value = os.getenv(env_key)
        if env_value is not None and env_value.strip():
            try:
                return float(env_value.strip())
            except ValueError:
                pass

        with self._lock:
            return self._config.getfloat(section, option, fallback=fallback)

    def get_bool(self, section: str, option: str, fallback=None):
        env_key = self._get_env_key(section, option)
        raw_env = os.getenv(env_key)

        if raw_env is not None:
            env_value = raw_env.strip().lower()
            if env_value in ("1", "true", "yes", "on", "t", "y"): return True
            if env_value in ("0", "false", "no", "off", "f", "n"): return False

        with self._lock:
            return self._config.getboolean(section, option, fallback=fallback)

    def check_required(self, required_params):
        """
        Verifica che una lista di parametri sia presente (in ENV o nel file).
        required_params: lista di tuple (sezione, opzione)
        """
        missing = []
        for section, option in required_params:
            if self.get(section, option) is None:
                missing.append(f"{section}.{option} (ENV: {section}_{option.upper()})")

        if missing:
            raise ValueError(f"❌ Configurazioni mancanti: {', '.join(missing)}")