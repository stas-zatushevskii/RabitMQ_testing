import logging 
from typing import TYPE_CHECKING
from datetime import datetime
from config import (
    get_connection,
    configure_logging,
    MQ_ROUTING_KEY,
    MQ_EXCHANGE
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel


log = logging.getLogger(__name__)


# создание сообщения:
def produce_message(chanel: "BlockingChannel") -> None:
    queue = chanel.queue_declare(queue=MQ_ROUTING_KEY)
    log.info("Declaraded queue %r: %s", MQ_ROUTING_KEY, queue)
    message_body = f"Hello world {datetime.now().strftime("%H:%M:%S")}"
    log.info("Pblish msg: %s", message_body)
    chanel.basic_publish(
        exchange=MQ_EXCHANGE,
        routing_key=MQ_ROUTING_KEY,
        body="Hello World!"
    )
    log.warning("Published message %s", message_body)


def main():
    configure_logging(level=logging.INFO)
    # создаем соединение: 
    with get_connection() as connection:
        log.info("Created connection: %s", connection)
        # создаем канал:
        with connection.channel() as chanel:
            log.info("Created chanel: %s", chanel)
            # отправляем сообщение по каналу
            produce_message(chanel=chanel)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Programm from console")
