import logging
from logging.handlers import TimedRotatingFileHandler
import os
from pathlib import Path

class LogRotator:
    """
    Classe per la configurazione semplificata di un logger con rotazione giornaliera.
    """

    def __init__(self, logger_name, log_path,log_file, log_level='INFO'):
        """
        Inizializza il logger.

        Args:
            logger_name (str): Il nome del logger.
            log_file_path (str): Il percorso completo del file di log (senza la data).
                                 E.g., '/percorso/del/tuo/log/app.log'
            log_level (int): Il livello di log desiderato (e.g., INFO, DEBUG) in formato stringa
        """
        self.logger_name = logger_name
        self.log_path = log_path
        self.log_file = log_file
        self.log_level = logging.getLevelName(log_level)

    def get_logger(self):
        """
        Configura e restituisce un'istanza del logger.

        Returns:
            logging.Logger: L'istanza del logger configurata.
        """
        # Creiamo un'istanza del logger
        logger = logging.getLogger(self.logger_name)
        logger.setLevel(self.log_level)

        # Evitiamo di aggiungere handler multipli se il logger è già stato configurato
        if not logger.handlers:
            # Creiamo la directory del log se non esiste
            if not self.log_path.exists():
                self.log_path.mkdir()
            #log_dir = os.path.dirname(self.log_file_path)
            # if not os.path.exists(log_dir):
            #     os.makedirs(log_dir)

            # Creiamo l'handler per la rotazione giornaliera
            # 'when='midnight'' e 'interval=1' per ruotare ogni giorno a mezzanotte
            # 'backupCount=7' per mantenere i log degli ultimi 7 giorni
            handler = TimedRotatingFileHandler(
                self.log_path / self.log_file,
                when='midnight',
                interval=1,
                backupCount=7,
                encoding='utf-8'
            )

            # Definiamo il formato del log
            formatter = logging.Formatter(
                '%(asctime)s - %(module)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            handler.setFormatter(formatter)

            # Aggiungiamo l'handler al logger
            logger.addHandler(handler)

            # Opzionale: aggiungiamo anche un handler per stampare su console
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

# if __name__ == '__main__':
    # Esempio di utilizzo
    # ------------------
    # Impostiamo il percorso del file di log (ad esempio, nella stessa directory dello script)
    # log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'my_app.log')
    #
    # # Creiamo un'istanza della classe e otteniamo il logger
    # log_config = LogRotator(
    #     logger_name='main_app_logger',
    #     log_file_path=log_path / log_file,
    #     log_level='DEBUG'
    # )
    # my_logger = log_config.get_logger()
    #
    # # Esempi di messaggi di log
    # my_logger.info("L'applicazione è stata avviata.")
    # my_logger.debug("Messaggio di debug: variabili inizializzate.")
    # my_logger.warning("Attenzione: una risorsa potrebbe non essere disponibile.")
    # my_logger.error("Errore: impossibile connettersi al database.")
    # my_logger.critical("Errore critico: il sistema sta per arrestarsi!")