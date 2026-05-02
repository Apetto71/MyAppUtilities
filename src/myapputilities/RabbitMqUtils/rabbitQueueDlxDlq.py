import pika as pk
import logging

# Configurazione del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


# --- Definizione della classe per gestire gli errori ---
class RabbitConnectionError(Exception):
    """Eccezione sollevata per problemi di connessione a RABBITMQ."""
    pass


# ------------------------------------------------------------------------

class RabbitMQConnection:
    """
    Una classe di utilità per gestire le connessioni a RabbitMQ,
    la pubblicazione e il consumo di messaggi.
    """

    MAX_RETRIES = 3
    RETRY_DELAY_MS = 5000

    def __init__(self, host: str, porta: int, user: str, passw: str):
        """
        Inizializzazione di istanza della classe per la connessione a rabbit
        """
        self.host = host
        self.porta = porta
        self.user = user
        self.passw = passw
        self.connection = None
        self.channel = None
        self.config_names = {}

    def connect(self):
        """
        Stabilisce una connessione con RabbitMQ e crea un canale.
        """
        credenziali = pk.PlainCredentials(self.user, self.passw)
        param_connection = pk.ConnectionParameters(host=self.host, port=self.porta, credentials=credenziali)
        try:
            self.connection = pk.BlockingConnection(param_connection)
            self.channel = self.connection.channel()
            logger.info("Connessione a RabbitMQ stabilita.")
            return True
        except pk.exceptions.AMQPConnectionError as e:
            logger.error(f"Errore di connessione a RabbitMQ: {e}")
            raise RabbitConnectionError(f"Errore nella connessione a RABBIT: {e}")

    # ... (get_connection, get_channel, close omessi per brevità)

    def publish_message(self, exchange_name, routing_key, message):
        """
        Pubblica un messaggio in un exchange specificato, rendendolo persistente.
        """
        if not self.channel:
            logger.error("Canale non disponibile. Impossibile pubblicare.")
            return

        # Assicura che l'Exchange esista e sia persistente
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

        # *** Messaggi Persistenti: delivery_mode=2 ***
        self.channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=message,
            properties=pk.BasicProperties(delivery_mode=2)
        )
        logger.info(f"Messaggio persistente pubblicato su exchange '{exchange_name}' con RK '{routing_key}'.")

    def consume_messages(self, queue_name, callback):
        """
        Consuma messaggi da una coda usando una funzione di callback.
        """
        if not self.channel:
            logger.error("Canale non disponibile. Impossibile consumare.")
            return

        # Assicura che la coda sia persistente (durable=True)
        # self.channel.queue_declare(queue=queue_name, durable=True)

        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=False
        )
        logger.info(f"In attesa di messaggi sulla coda '{queue_name}'. Premi CTRL+C per uscire.")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Consumo interrotto dall'utente.")
        except Exception as e:
            logger.error(f"Errore durante il consumo: {e}")

    # --- METODO DI SETUP CON PERSISTENZA E NOMI AUTOMATICI ---

    def setup_retry_and_dlq_queues(self, main_exchange: str, main_queue: str, routing_key: str):
        """
        Configura la coda principale, la coda di retry e la Dead Letter Queue (DLQ).
        Tutti gli elementi (Exchange, Code) sono dichiarati come PERSISTENTI (durable=True).

        :param main_exchange: Exchange principale al quale si legheranno le code.
        :param main_queue: Nome della coda principale.
        :param routing_key: Routing key usata per l'exchange principale.
        """
        if not self.channel:
            logger.error("Canale non disponibile. Impossibile configurare le code.")
            return

        # Nomi derivati automaticamente
        RETRY_QUEUE = f"{main_queue}_retry_queue"
        DLQ_QUEUE = f"{main_queue}_dlq"
        DLQ_EXCHANGE = f"{main_exchange}_dlx"
        DLQ_ROUTING_KEY = routing_key  # Usiamo la RK principale anche per il DLX per semplicità

        # Memorizza i nomi per l'uso nel metodo requeue_or_dlq
        self.config_names = {
            'main_exchange': main_exchange,
            'retry_queue': RETRY_QUEUE,
            'dlq_exchange': DLQ_EXCHANGE,
            'dlq_routing_key': DLQ_ROUTING_KEY,
            'dlq_queue': DLQ_QUEUE
        }

        try:
            # 1. DICHIARAZIONE DELL'EXCHANGE PRINCIPALE (Persistente)
            self.channel.exchange_declare(
                exchange=main_exchange,
                exchange_type='direct',
                durable=True  # <--- PERSISTENZA
            )
            logger.info(f"Exchange Principale PERSISTENTE '{main_exchange}' dichiarato.")

            # 2. DLQ (Dead Letter Queue) SETUP (Exchange e Coda Persistenti)
            self.channel.exchange_declare(
                exchange=DLQ_EXCHANGE,
                exchange_type='direct',
                durable=True  # <--- PERSISTENZA
            )
            self.channel.queue_declare(queue=DLQ_QUEUE, durable=True)  # <--- PERSISTENZA
            self.channel.queue_bind(
                exchange=DLQ_EXCHANGE,
                queue=DLQ_QUEUE,
                routing_key=DLQ_ROUTING_KEY
            )
            logger.info(f"DLX/DLQ PERSISTENTI '{DLQ_EXCHANGE}' e '{DLQ_QUEUE}' configurati.")

            # 3. RETRY QUEUE SETUP (Persistente)
            retry_arguments = {
                'x-dead-letter-exchange': main_exchange,
                'x-dead-letter-routing-key': routing_key,
                'x-message-ttl': self.RETRY_DELAY_MS
            }
            self.channel.queue_declare(
                queue=RETRY_QUEUE,
                durable=True,  # <--- PERSISTENZA
                arguments=retry_arguments
            )

            # Binding della Retry Queue (Persistente)
            self.channel.queue_bind(
                exchange=main_exchange,
                queue=RETRY_QUEUE,
                routing_key=RETRY_QUEUE
            )
            logger.info(f"Retry Queue PERSISTENTE '{RETRY_QUEUE}' configurata.")

            # 4. CODA PRINCIPALE SETUP (Persistente)
            main_queue_arguments = {
                'x-dead-letter-exchange': DLQ_EXCHANGE,
                'x-dead-letter-routing-key': RETRY_QUEUE
            }
            self.channel.queue_declare(
                queue=main_queue,
                durable=True,  # <--- PERSISTENZA
                arguments=main_queue_arguments
            )
            # Binding della Coda Principale (Persistente)
            self.channel.queue_bind(
                exchange=main_exchange,
                queue=main_queue,
                routing_key=routing_key
            )
            logger.info(f"Coda Principale PERSISTENTE '{main_queue}' configurata.")

        except Exception as e:
            logger.error(f"Errore nella configurazione delle code e DLQ: {e}")
            raise

    # Il metodo requeue_or_dlq rimane invariato, in quanto usa i nomi generati
    # e la logica ACK/NACK è indipendente dalla persistenza delle code.
    def requeue_or_dlq(self, channel, method, properties, body):
        """
        Logica per decidere se un messaggio deve essere ritentato o inviato alla DLQ.
        """
        if not self.config_names:
            logger.error("Errore: La configurazione delle code DLQ/Retry non è stata eseguita.")
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            return

        main_exchange = self.config_names['main_exchange']
        dlq_exchange_name = self.config_names['dlq_exchange']
        dlq_routing_key = self.config_names['dlq_routing_key']
        retry_queue_name = self.config_names['retry_queue']

        # Controlla l'header 'x-death' per RabbitMQ per contare i tentativi.
        retry_count = 0
        for death in properties.headers.get('x-death', []):
            if death.get('exchange') == main_exchange and death.get('reason') == 'expired':
                retry_count = death.get('count', 0)
                break

        current_attempt = retry_count + 1

        if current_attempt <= self.MAX_RETRIES:
            # Riprova: NACK con requeue=False (invia al DLX/Retry Queue)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            logger.warning(
                f"Messaggio {method.delivery_tag} fallito, tentativo #{current_attempt}/{self.MAX_RETRIES}. Rinvio a '{retry_queue_name}'.")

        else:
            # DLQ: massimo numero di tentativi raggiunto
            logger.error(f"Messaggio {method.delivery_tag} ha superato {self.MAX_RETRIES} tentativi. Invio alla DLQ.")

            # 1. Riconosce (ACK) il messaggio per rimuoverlo dalla coda principale
            channel.basic_ack(delivery_tag=method.delivery_tag)

            # 2. Pubblica manualmente sul DLX finale
            channel.basic_publish(
                exchange=dlq_exchange_name,
                routing_key=dlq_routing_key,
                body=body,
                # Pubblicazione persistente anche sul DLQ
                properties=pk.BasicProperties(delivery_mode=2, headers={'X-Original-Exchange': main_exchange,
                                                                        'X-Original-Routing-Key': properties.routing_key})
            )

            logger.info(f"Messaggio {method.delivery_tag} ACK sulla main queue e pubblicato su DLQ.")


# Esempio di utilizzo (invariato)
if __name__ == '__main__':

    MAIN_EXCHANGE = 'invest_exchange'
    MAIN_QUEUE = 'invest_main_queue'
    ROUTING_KEY = 'invest_rk'

    connessione = RabbitMQConnection('localhost', 5672, 'guest', 'guest')
    try:
        connessione.connect()
    except RabbitConnectionError as e:
        print(e)
        exit()

    connessione.setup_retry_and_dlq_queues(
        main_exchange=MAIN_EXCHANGE,
        main_queue=MAIN_QUEUE,
        routing_key=ROUTING_KEY
    )


    def my_callback(channel, method, properties, body):
        logger.info(f"Ricevuto: {body.decode()}")

        if "FAIL" in body.decode():
            connessione.requeue_or_dlq(channel, method, properties, body)
        else:
            logger.info(f"Messaggio {method.delivery_tag} processato con successo.")
            channel.basic_ack(delivery_tag=method.delivery_tag)


    connessione.publish_message(
        exchange_name=MAIN_EXCHANGE,
        routing_key=ROUTING_KEY,
        message=b'Messaggio di test SUCCESS'
    )
    connessione.publish_message(
        exchange_name=MAIN_EXCHANGE,
        routing_key=ROUTING_KEY,
        message=b'Messaggio di test FAIL'
    )

    try:
        connessione.consume_messages(queue_name=MAIN_QUEUE, callback=my_callback)
    except Exception as e:
        logger.error(f"Errore durante il consumo: {e}")
    finally:
        connessione.close()