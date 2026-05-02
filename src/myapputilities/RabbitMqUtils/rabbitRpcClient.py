import time
import json
import pika as pk
import logging
import uuid
import threading
from RabbitMqUtils.rabbitQueueDlq import RabbitConnectionError
from typing import Optional # Aggiungere in cima al file se non presente

logger = logging.getLogger(__name__)

class RabbitMQRpcClient(object):
    """
    Utility Client per le chiamate RPC sincrone via RabbitMQ.
    Invia un messaggio e attende la risposta su una coda 'reply_to'.
    """

    def __init__(self, host: str, porta: int, user: str, passw: str):
        self.host = host
        self.porta = porta
        self.user = user
        self.passw = passw
        self.connection = None
        self.channel = None
        self.corr_id = None
        self.response = None
        self.lock = threading.Lock() # Lock per la sincronizzazione

    def connect(self):
        """Stabilisce una connessione con RabbitMQ e crea un canale."""
        credenziali = pk.PlainCredentials(self.user, self.passw)
        param_connection = pk.ConnectionParameters(
            host=self.host,
            port=self.porta,
            credentials=credenziali
        )
        try:
            self.connection = pk.BlockingConnection(param_connection)
            self.channel = self.connection.channel()
            logger.info("Connessione RPC a RabbitMQ stabilita.")
            return True
        except pk.exceptions.AMQPConnectionError as e:
            logger.error(f"Errore di connessione RPC a RabbitMQ: {e}")
            raise RabbitConnectionError(f"Errore nella connessione RPC: {e}")

    def close(self):
        """Chiude la connessione a RabbitMQ."""
        if self.connection and self.connection.is_open:
            try:
                self.connection.close()
                logger.info("Connessione RPC a RabbitMQ chiusa.")
            except Exception as e:
                logger.warning(f"Errore durante la chiusura pulita RPC: {e}")
        self.connection = None
        self.channel = None

    def on_response(self, ch, method, props, body):
        """Callback eseguita quando arriva la risposta."""
        # Se l'ID di correlazione corrisponde, salva la risposta
        if self.corr_id == props.correlation_id:
            with self.lock:
                self.response = body
            # Interrompi il consumo (esci dal loop)
            ch.stop_consuming()

    # def call(self, queue_name, message: str, timeout: int = 15, headers: dict = None): # <--- MODIFICA 1: Aggiunto 'headers'
    #     """
    #     Esegue una chiamata RPC.
    #     ... (omissis) ...
    #     """
    #     # Se la connessione è persa, tentiamo di ristabilirla.
    #     if self.channel is None or self.channel.is_closed:
    #         logger.warning("Canale RPC non disponibile/chiuso. Tentativo di riconnessione.")
    #
    #         # Chiudiamo la connessione precedente se non è già chiusa, per sicurezza
    #         if self.connection and not self.connection.is_closed:
    #             try:
    #                 self.connection.close()
    #             except Exception:
    #                 # Ignoriamo errori di chiusura se il socket è già rotto
    #                 pass
    #
    #         # Tentativo di riconnessione. Solleva RabbitConnectionError in caso di fallimento.
    #         try:
    #             self.connect()
    #         except RabbitConnectionError as e:
    #             logger.error(f"Riconnessione RPC fallita: {e}")
    #             raise
    #
    #         if self.channel is None or self.channel.is_closed:
    #             raise RabbitConnectionError("Riconnessione RPC fallita. Impossibile eseguire la chiamata.")
    #     # =========================================================================
    #
    #     self.response = None
    #     self.corr_id = str(uuid.uuid4())
    #
    #     # 1. Definisce la coda temporanea 'reply_to'
    #     # ... (Il resto del codice originale segue da qui)
    #     result = self.channel.queue_declare(queue='', exclusive=True)
    #     if not self.channel or not self.channel.is_open:
    #         raise RabbitConnectionError("Canale RPC non disponibile.")
    #
    #     self.response = None
    #     self.corr_id = str(uuid.uuid4())
    #
    #     # 1. Dichiarazione della coda di risposta (esclusiva, auto-cancellante)
    #     result = self.channel.queue_declare(queue='', exclusive=True)
    #     self.callback_queue = result.method.queue
    #
    #     # 2. Configura il consumer per la coda di risposta
    #     self.channel.basic_consume(
    #         queue=self.callback_queue,
    #         on_message_callback=self.on_response,
    #         auto_ack=True
    #     )
    #
    #     # --- MODIFICA 2: Costruzione dinamica delle proprietà RPC ---
    #     properties_args = {
    #         'reply_to': self.callback_queue,
    #         'correlation_id': self.corr_id,
    #     }
    #
    #     # Includi gli headers se forniti
    #     if headers is not None:
    #         properties_args['headers'] = headers
    #
    #     # 3. Pubblica la richiesta con le proprietà RPC
    #     self.channel.basic_publish(
    #         exchange='',
    #         routing_key=queue_name,
    #         properties=pk.BasicProperties(**properties_args),  # <--- MODIFICA 3: Usa il dizionario costruito
    #         body=message.encode('utf-8')
    #     )
    #     logger.info(f"Richiesta RPC inviata a '{queue_name}' con ID: {self.corr_id}")
    #
    #     # 4. Ciclo di attesa (Metodo più robusto per BlockingConnection)
    #     start_time = time.time()
    #
    #     while self.response is None and (time.time() - start_time) < timeout:
    #         # Attendiamo passivamente gli eventi (1 secondo alla volta)
    #         # Questo è il modo corretto per Pika per fare un'attesa con timeout
    #         self.connection.process_data_events(time_limit=1)
    #
    #         # Nota: se la on_response riceve il messaggio, chiama ch.stop_consuming(),
    #         # che interromperà il loop interno di process_data_events se era in esecuzione,
    #         # e imposterà self.response.
    #
    #     if self.response is None:
    #         # Se usciamo dal ciclo per timeout, dobbiamo anche rimuovere il consumer
    #         # per liberare la coda temporanea, anche se è esclusiva.
    #         self.channel.queue_delete(queue=self.callback_queue)
    #         logger.error(f"Timeout o nessuna risposta ricevuta per RPC ID: {self.corr_id}")
    #         raise TimeoutError("Timeout scaduto in attesa di risposta RPC.")
    #
    #     # Elimina la coda temporanea dopo l'uso
    #     self.channel.queue_delete(queue=self.callback_queue)
    #
    #     logger.info(f"Risposta RPC ricevuta per ID: {self.corr_id}")
    #     return self.response
    def call(self, queue_name, message: str, timeout: int = 15, headers: dict = None):
        """Esegue una chiamata RPC con logica di retry in caso di StreamLostError (ConnectionResetError)."""

        # Tentativo massimo di 2 volte (1 normale + 1 retry)
        MAX_ATTEMPTS = 2

        for attempt in range(MAX_ATTEMPTS):
            try:
                # =========================================================================
                # 1. GESTIONE STATO E RICONNESSIONE PREVENTIVA
                # Se la connessione è palesemente inattiva o chiusa, prova a ristabilirla.
                if self.channel is None or self.connection is None or not self.connection.is_open:
                    logger.warning(f"Tentativo {attempt + 1}: Canale RPC non disponibile/chiuso. Riconnessione...")

                    # Pulizia aggressiva della vecchia istanza, specialmente dopo un StreamLostError al Tentativo 1.
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
                        raise RabbitConnectionError("Riconnessione RPC fallita. Impossibile eseguire la chiamata.")
                # =========================================================================

                # Inizializza/Reset dei parametri per la chiamata
                self.response = None
                self.corr_id = str(uuid.uuid4())

                # 2. Dichiarazione della coda di risposta (esclusiva)
                # Questa linea scatena l'errore StreamLostError se la connessione è "zombie"
                result = self.channel.queue_declare(queue='', exclusive=True)
                self.callback_queue = result.method.queue

                # 3. Configura il consumer
                self.channel.basic_consume(
                    queue=self.callback_queue,
                    on_message_callback=self.on_response,
                    auto_ack=True
                )

                # 4. Pubblica la richiesta
                properties_args = {
                    'reply_to': self.callback_queue,
                    'correlation_id': self.corr_id,
                }
                if headers is not None:
                    properties_args['headers'] = headers

                self.channel.basic_publish(
                    exchange='',
                    routing_key=queue_name,
                    properties=pk.BasicProperties(**properties_args),
                    body=message.encode('utf-8')
                )
                logger.info(f"Richiesta RPC inviata a '{queue_name}' con ID: {self.corr_id}")

                # 5. Ciclo di attesa (Attende la risposta con timeout)
                start_time = time.time()
                while self.response is None and (time.time() - start_time) < timeout:
                    self.connection.process_data_events(time_limit=1)

                # 6. Pulizia e Ritorno
                try:
                    # Elimina la coda temporanea dopo l'uso
                    self.channel.queue_delete(queue=self.callback_queue)
                except Exception:
                    pass  # Ignora errori di pulizia su canale potenzialmente rotto

                if self.response is None:
                    logger.error(f"Timeout o nessuna risposta ricevuta per RPC ID: {self.corr_id}")
                    raise TimeoutError("Timeout scaduto in attesa di risposta RPC.")

                logger.info(f"Risposta RPC ricevuta per ID: {self.corr_id}")
                return self.response  # Successo: esci dalla funzione e dal loop


            except pk.exceptions.StreamLostError as e:
                # CATTURA DELL'ERRORE: La connessione è morta al primo uso (ConnectionReset)
                if attempt < MAX_ATTEMPTS - 1:
                    logger.warning(
                        f"Stream RPC perso durante la chiamata. Riprovo ({attempt + 1}/{MAX_ATTEMPTS - 1})...")
                    self.connection = None
                    self.channel = None
                    continue
                else:
                    logger.error(
                        f"Errore StreamLostError non recuperabile dopo {MAX_ATTEMPTS} tentativi. Chiamata fallita: {e}")
                    raise RabbitConnectionError(f"Errore RPC non recuperabile: {e}") from e

            except Exception as e:
                # Cattura tutti gli altri errori
                raise e

    def on_stream_response(self, ch, method, props, body):
        """Callback per gestire i chunk in arrivo nell'AI Service"""
        if self.corr_id == props.correlation_id:

            # 1. Verifica immediata del segnale di chiusura (Header)
            if props.headers and props.headers.get('stream-end') is True:
                logger.info(f"✅ EOF ricevuto via Header per ID: {self.corr_id}")
                self.stream_finished = True
                return

            try:
                # 2. Decodifica del contenuto
                decoded_body = body.decode('utf-8')
                data = json.loads(decoded_body)

                # 3. Controllo di sicurezza se l'EOF fosse nel body
                if isinstance(data, dict) and data.get('status') == 'EOF':
                    logger.info(f"✅ EOF ricevuto via Body per ID: {self.corr_id}")
                    self.stream_finished = True
                    return

                # 4. Accumulo dei dati effettivi
                with self.lock:
                    if isinstance(data, list):
                        self.response.extend(data)
                    else:
                        self.response.append(data)

            except Exception as e:
                logger.error(f"❌ Errore processamento chunk: {e}")

    def call_stream(self, queue_name, message: str, timeout: int = 300, headers: dict = None):
        self.response = []  # Resetta la lista dei dati
        self.stream_finished = False  # Resetta il flag di fine
        self.corr_id = str(uuid.uuid4())

        if self.channel is None or not self.connection.is_open:
            self.connect()

        # Creiamo una coda di risposta temporanea ed esclusiva
        result = self.channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue

        # Registriamo la callback specifica per lo streaming
        self.channel.basic_consume(
            queue=callback_queue,
            on_message_callback=self.on_stream_response,
            auto_ack=True
        )

        # Invio della richiesta con gli header necessari (x-request-activity)
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pk.BasicProperties(
                reply_to=callback_queue,
                correlation_id=self.corr_id,
                headers=headers
            ),
            body=message.encode('utf-8')
        )

        logger.info(f"Avviata chiamata streaming RPC ID: {self.corr_id} su coda {queue_name}")

        start_time = time.time()
        # Loop di attesa: continua finché il server non manda 'stream-end'
        while not self.stream_finished:
            self.connection.process_data_events(time_limit=1)

            if (time.time() - start_time) > timeout:
                # Pulizia coda in caso di errore
                self.channel.queue_delete(queue=callback_queue)
                logger.error(f"Timeout streaming RPC per ID: {self.corr_id}")
                raise TimeoutError("Il server non ha terminato lo streaming entro il tempo limite.")

        # Pulizia finale della coda temporanea
        try:
            self.channel.queue_delete(queue=callback_queue)
        except Exception:
            pass

        return self.response