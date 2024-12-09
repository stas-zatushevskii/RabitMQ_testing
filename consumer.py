import logging 
from typing import TYPE_CHECKING
import time
from config import (
    get_connection,
    configure_logging,
    MQ_ROUTING_KEY,
    MQ_EXCHANGE
)

if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika.spec import Basic, BasicProperties


log = logging.getLogger(__name__)


def process_new_message(
        ch: "BlockingChannel",
        method: "Basic.Deliver",
        properties: "BasicProperties",
        body: bytes
        ):
    log.debug("ch: %s", ch)
    log.debug("method: %s", method)
    log.debug("properties: %s", properties)
    log.debug("body: %s", body)

    log.info("Started processing message")
    start_time = time.time()
    time.sleep(5)
    end_time = time.time()
    ch.basic_ack(delivery_tag=method.delivery_tag)
    log.warning(
        "Finished in %.2fs processing message %r ",
        start_time-end_time,
        body
    )


# получение сообщения:
def consume_message(chanel: "BlockingChannel") -> None:
    chanel.basic_consume(
        queue=MQ_ROUTING_KEY,
        on_message_callback=process_new_message,
        # auto_ack=True
    )
    log.warning("Waiting for messages")
    chanel.start_consuming()


def main():
    configure_logging(level=logging.INFO)
    # создаем соединение: 
    with get_connection() as connection:
        log.info("Created connection: %s", connection)
        # создаем канал:
        with connection.channel() as chanel:
            log.info("Created chanel: %s", chanel)
            # отправляем сообщение по каналу
            consume_message(chanel=chanel)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        log.warning("Programm from console")
