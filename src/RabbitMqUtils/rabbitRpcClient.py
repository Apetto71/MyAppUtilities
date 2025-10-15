import time

import pika as pk
import logging
import uuid
import threading
from RabbitMqUtils.rabbitQueueDlq import RabbitConnectionError

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

    def call(self, queue_name, message: str, timeout: int = 15):
        """
        Esegue una chiamata RPC.
        ... (omissis) ...
        """
        if not self.channel or not self.channel.is_open:
            raise RabbitConnectionError("Canale RPC non disponibile.")

        self.response = None
        self.corr_id = str(uuid.uuid4())

        # 1. Dichiarazione della coda di risposta (esclusiva, auto-cancellante)
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # 2. Configura il consumer per la coda di risposta
        # L'uso di auto_ack=True è comune in RPC
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

        # 3. Pubblica la richiesta con le proprietà RPC
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pk.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=message.encode('utf-8')
        )
        logger.info(f"Richiesta RPC inviata a '{queue_name}' con ID: {self.corr_id}")

        # 4. Ciclo di attesa (Metodo più robusto per BlockingConnection)
        start_time = time.time()

        while self.response is None and (time.time() - start_time) < timeout:
            # Attendiamo passivamente gli eventi (1 secondo alla volta)
            # Questo è il modo corretto per Pika per fare un'attesa con timeout
            self.connection.process_data_events(time_limit=1)

            # Nota: se la on_response riceve il messaggio, chiama ch.stop_consuming(),
            # che interromperà il loop interno di process_data_events se era in esecuzione,
            # e imposterà self.response.

        if self.response is None:
            # Se usciamo dal ciclo per timeout, dobbiamo anche rimuovere il consumer
            # per liberare la coda temporanea, anche se è esclusiva.
            self.channel.queue_delete(queue=self.callback_queue)
            logger.error(f"Timeout o nessuna risposta ricevuta per RPC ID: {self.corr_id}")
            raise TimeoutError("Timeout scaduto in attesa di risposta RPC.")

        # Elimina la coda temporanea dopo l'uso
        self.channel.queue_delete(queue=self.callback_queue)

        logger.info(f"Risposta RPC ricevuta per ID: {self.corr_id}")
        return self.response