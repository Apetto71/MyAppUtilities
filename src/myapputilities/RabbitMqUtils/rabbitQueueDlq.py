import pika as pk
import logging
import threading # <-- Importazione Aggiunta!
from typing import Optional

logger = logging.getLogger(__name__)

# --- Definizione della classe per gestire gli errori ---
class RabbitConnectionError(Exception):
    """Eccezione sollevata per problemi di connessione a RABBITMQ."""
    pass

class ConsumptionStoppedError(Exception):
    """Eccezione interna sollevata quando il consumo si ferma per StreamLost o altro."""
    pass
# --------------------------------------------------------

class RabbitMQSimpleDLQ(object):
    """
    Utility per la connessione, l'Exchange/Queue setup e il consumo bloccante.
    """

    def __init__(self, host: str, porta: int, user: str, passw: str):
        self.host = host
        self.porta = porta
        self.user = user
        self.passw = passw
        self.connection = None
        self.channel = None
        self.config_names = {}
        # Flag per segnalare l'interruzione al thread di consumo
        self._should_stop = threading.Event()

    import pika as pk
    import logging
    import threading
    # Rimuovi l'import di 'socket' se non lo usi altrove

    # ... (Definizione delle classi) ...

    def connect(self):
        """Stabilisce una connessione con RabbitMQ e crea un canale."""
        credenziali = pk.PlainCredentials(self.user, self.passw)

        # RIMUOVE IL PARAMETRO socket_settings CHE CAUSA L'ERRORE
        param_connection = pk.ConnectionParameters(
            host=self.host,
            port=self.porta,
            credentials=credenziali
        )
        try:
            self.connection = pk.BlockingConnection(param_connection)

            # --------------------------------------------------------------------
            # NUOVO TENTATIVO DI SOLUZIONE:
            # Implichiamo che il problema sia risolto dalle precedenti correzioni.
            # Se la BodyTooLongError (il vero problema di consumo) persiste,
            # dovremo passare a una connessione Asincrona o rinegoziare i limiti.
            # --------------------------------------------------------------------

            self.channel = self.connection.channel()
            # Resetta il flag di stop ad ogni nuova connessione
            self._should_stop.clear()
            logger.info("Connessione a RabbitMQ stabilita.")
            return True
        except pk.exceptions.AMQPConnectionError as e:
            logger.error(f"Errore di connessione a RabbitMQ: {e}")
            raise RabbitConnectionError(f"Errore nella connessione a RABBIT: {e}")

    def close(self):
        """Chiude la connessione a RabbitMQ."""
        if self.connection and self.connection.is_open:
            # Imposta il flag per segnalare l'interruzione al consumatore
            self._should_stop.set()
            try:
                # Chiama stop_consuming per uscire dal loop bloccante
                if self.channel and self.channel.is_open:
                     self.channel.stop_consuming()
                self.connection.close()
                logger.info("Connessione a RabbitMQ chiusa.")
            except Exception as e:
                logger.warning(f"Errore durante la chiusura pulita: {e}")
        self.connection = None
        self.channel = None

    # def publish_message(self, exchange_name, routing_key, message, headers=None):
    #     """
    #     Pubblica un messaggio persistente in un exchange specificato, supportando gli Headers.
    #
    #     Args:
    #         exchange_name (str): Nome dell'Exchange.
    #         routing_key (str): Routing Key.
    #         message (bytes): Corpo del messaggio.
    #         headers (dict, optional): Dizionario di metadati personalizzati da allegare.
    #     """
    #     if not self.channel or not self.channel.is_open:
    #         logger.warning("Canale non aperto o disponibile. Riprovo la connessione...")
    #         # Qui potresti aggiungere una logica di riconnessione se necessario
    #         raise RabbitConnectionError("Canale non disponibile per la pubblicazione.")
    #
    #     # Prepara le proprietà
    #     basic_properties_args = {
    #         'delivery_mode': 2,  # Rende il messaggio persistente
    #     }
    #
    #     # Aggiunge gli headers se sono forniti
    #     if headers is not None:
    #         basic_properties_args['headers'] = headers
    #
    #     properties = pk.BasicProperties(**basic_properties_args)
    #
    #     try:
    #         # 1. Pubblica il messaggio
    #         self.channel.basic_publish(
    #             exchange=exchange_name,
    #             routing_key=routing_key,
    #             body=message,
    #             # 2. Imposta proprietà per rendere il messaggio persistente
    #             properties=properties
    #             )
    #
    #         # Nota: Non loggare ogni singolo publish, altrimenti la log è enorme
    #         # logger.debug(f"Messaggio pubblicato su {exchange_name} con key {routing_key}")
    #
    #     except pk.exceptions.AMQPChannelError as e:
    #         logger.error(f"Errore di canale durante la pubblicazione: {e}")
    #         raise
    #     except pk.exceptions.AMQPConnectionError as e:
    #         logger.error(f"Errore di connessione durante la pubblicazione: {e}")
    #         raise RabbitConnectionError(f"Connessione persa durante la pubblicazione: {e}")
    #     except Exception as e:
    #         logger.error(f"Errore generico durante la pubblicazione: {e}")
    #         raise

    def publish_message(self, exchange_name, routing_key, message: bytes, headers: Optional[dict] = None):
        """
        Pubblica un messaggio, con logica di retry in caso di StreamLostError.
        """
        MAX_ATTEMPTS = 2

        for attempt in range(MAX_ATTEMPTS):
            try:
                # 1. GESTIONE STATO E RICONNESSIONE PREVENTIVA
                # Se la connessione è palesemente inattiva o chiusa, prova a ristabilirla.
                if self.channel is None or self.connection is None or not self.connection.is_open:
                    logger.warning(f"Tentativo {attempt + 1}: Canale DLQ non disponibile/chiuso. Riconnessione...")

                    # Pulizia aggressiva della vecchia istanza, se necessario
                    if self.connection and self.connection.is_open:
                        try:
                            self.connection.close()
                        except Exception:
                            pass

                    # Forza il client a creare nuove istanze di connessione e canale
                    self.connection = None
                    self.channel = None

                    self.connect()  # Solleva RabbitConnectionError in caso di fallimento.

                    if self.channel is None or self.channel.is_closed:
                        raise RabbitConnectionError("Riconnessione DLQ fallita. Impossibile eseguire la pubblicazione.")

                # 2. ESECUZIONE DELLA PUBBLICAZIONE
                self.channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=pk.BasicProperties(
                        delivery_mode=pk.spec.PERSISTENT_DELIVERY_MODE,
                        headers=headers or {}
                    )
                )
                logger.debug(f"Messaggio pubblicato su {exchange_name} con RK: {routing_key}")
                return  # Successo: esci dalla funzione e dal loop

            except pk.exceptions.StreamLostError as e:
                # CATTURA DELL'ERRORE: La connessione è morta al primo uso (ConnectionReset)
                if attempt < MAX_ATTEMPTS - 1:
                    logger.warning(
                        f"Stream DLQ perso durante la pubblicazione. Riprovo ({attempt + 1}/{MAX_ATTEMPTS - 1})...")
                    # Pulizia aggressiva dello stato rotto per il prossimo tentativo
                    self.connection = None
                    self.channel = None
                    continue  # Passa al prossimo ciclo (tentativo di retry)
                else:
                    logger.error(
                        f"Errore StreamLostError non recuperabile dopo {MAX_ATTEMPTS} tentativi. Pubblicazione fallita: {e}")
                    # Solleva l'errore per il gestore chiamante (gestore_rabbit.salva_dati)
                    raise RabbitConnectionError(f"Connessione persa durante la pubblicazione: {e}") from e

            except Exception as e:
                # Cattura altri errori di pubblicazione (es. ChannelClosedByBroker)
                raise RabbitConnectionError(f"Errore inatteso durante la pubblicazione: {e}") from e

    def setup_main_and_dlq(self, main_exchange: str, main_queue: str, routing_key: str):
        """
        Configura l'infrastruttura di RabbitMQ: Main Exchange, Main Queue e il sistema DLX/DLQ.

        Args:
            main_exchange: Il nome dell'Exchange a cui il Producer pubblicherà.
            main_queue: Il nome della Coda principale da cui il Consumer leggerà.
            routing_key: La Routing Key usata per collegare gli elementi.
        """
        if not self.channel or not self.channel.is_open:
            logger.error("Canale non disponibile per la configurazione.")
            raise RabbitConnectionError("Canale non disponibile per setup code.")

        # Nomi per il DLX e la DLQ (basati sul nome della coda principale, come da logica)
        dlx_name = f"{main_queue}_dlx"
        dlq_name = f"{main_queue}_dlq"

        try:
            # 1. SETUP DLX/DLQ
            # Dichiarazione del Dead Letter Exchange (DEVE ESSERCI)
            self.channel.exchange_declare(
                exchange=dlx_name,
                exchange_type='direct',  # Usiamo 'direct' per prevenire l'errore PRECONDITION_FAILED
                durable=True
            )
            logger.info(f"DLX dichiarato (tipo 'direct'): {dlx_name}")

            # Dichiarazione della Dead Letter Queue (DLQ)
            self.channel.queue_declare(
                queue=dlq_name,
                durable=True
            )
            logger.info(f"DLQ dichiarata: {dlq_name}")

            # Binding tra DLQ e DLX
            self.channel.queue_bind(
                exchange=dlx_name,
                queue=dlq_name,
                routing_key=routing_key
            )
            logger.info(f"Binding DLQ <-> DLX effettuato: {routing_key}")

            # 2. SETUP MAIN EXCHANGE/QUEUE (NECESSARIO PER IL PRODUCER)

            # Dichiarazione del Main Exchange (Riceve i messaggi dal Producer)
            self.channel.exchange_declare(
                exchange=main_exchange,
                exchange_type='topic',  # Mantengo 'topic' come avevi nel codice originale
                durable=True
            )
            logger.info(f"Main Exchange dichiarato: {main_exchange}")

            # Dichiarazione della Main Queue, con l'argomento 'dead-letter-exchange'
            self.channel.queue_declare(
                queue=main_queue,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': dlx_name,
                    'x-dead-letter-routing-key': routing_key  # Opzionale, ma sicuro
                }
            )
            logger.info(f"Main Queue dichiarata con DLX: {main_queue}")

            # Binding tra Main Queue e Main Exchange (Collega il Producer al Consumer)
            self.channel.queue_bind(
                exchange=main_exchange,
                queue=main_queue,
                routing_key=routing_key
            )
            logger.info(f"Binding Main Queue <-> Main Exchange effettuato: {routing_key}")

        except pk.exceptions.AMQPError as e:
            logger.error(f"Errore AMQP durante il setup delle code: {e}")
            raise RabbitConnectionError(f"Errore AMQP nel setup: {e}")

    def consume_messages(self, queue_name, callback):
        """
        Consuma messaggi in modo bloccante.
        Solleva ConsumptionStoppedError in caso di perdita di connessione per forzare la riconnessione esterna.
        """
        if not self.channel or self._should_stop.is_set():
            logger.error("Canale non disponibile o stop richiesto. Impossibile consumare.")
            return

        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False
        )
        logger.info(f"In attesa di messaggi sulla coda '{queue_name}'. Premi CTRL+C per uscire.")
        try:
            # Start_consuming è bloccante.
            self.channel.start_consuming()
        except pk.exceptions.StreamLostError as e:
            # Errore di connessione a livello di socket (es. BodyTooLongError)
            logger.error(f"Errore critico di RabbitMQ: Connessione persa: {e}")
            raise ConsumptionStoppedError(f"Stream connection lost: {e}")