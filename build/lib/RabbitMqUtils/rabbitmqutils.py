import pika


class RabbitMQConnection:
    """
    Una classe di utilità per gestire le connessioni a RabbitMQ,
    la pubblicazione e il consumo di messaggi.
    """

    def __init__(self, host='localhost'):
        """
        Inizializza la classe con l'host di RabbitMQ.
        """
        self.host = host
        self.connection = None
        self.channel = None

    def connect(self):
        """
        Stabilisce una connessione con RabbitMQ e crea un canale.
        """
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.channel = self.connection.channel()
            print("Connessione a RabbitMQ stabilita.")
            return True
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Errore di connessione a RabbitMQ: {e}")
            return False

    def close(self):
        """
        Chiude la connessione a RabbitMQ se è aperta.
        """
        if self.connection and self.connection.is_open:
            self.connection.close()
            print("Connessione a RabbitMQ chiusa.")

    def publish_message(self, queue_name, message):
        """
        Pubblica un messaggio in una coda specificata.
        """
        if not self.channel:
            print("Canale non disponibile. Impossibile pubblicare.")
            return

        # Crea la coda se non esiste
        self.channel.queue_declare(queue=queue_name)

        # Pubblica il messaggio
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=message
        )
        print(f"Messaggio pubblicato: '{message}' sulla coda '{queue_name}'.")

    def consume_messages(self, queue_name, callback):
        """
        Consuma messaggi da una coda usando una funzione di callback.
        """
        if not self.channel:
            print("Canale non disponibile. Impossibile consumare.")
            return

        self.channel.queue_declare(queue=queue_name)
        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=callback,
            auto_ack=True  # Riconoscimento automatico del messaggio
        )
        print(f"In attesa di messaggi sulla coda '{queue_name}'. Premi CTRL+C per uscire.")
        self.channel.start_consuming()