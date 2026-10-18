from aio_pika import RobustExchange
from faststream.rabbit import ExchangeType, QueueType, RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas.queue import QuorumQueueArgs

PAYMENTS_EXCHANGE_NAME = "payments"
RETRY_EXCHANGE_NAME = "payments.retry"
DEAD_LETTER_EXCHANGE_NAME = "payments.dlx"

PAYMENTS_NEW_QUEUE_NAME = "payments.new"
PAYMENTS_NEW_ROUTING_KEY = "payments.new"
DEAD_LETTER_QUEUE_NAME = "payments.new.dlq"
DEAD_LETTER_ROUTING_KEY = "payments.new.dlq"

ATTEMPT_HEADER = "x-processing-attempt"

PAYMENTS_EXCHANGE = RabbitExchange(PAYMENTS_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange(RETRY_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange(
    DEAD_LETTER_EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True
)


def _dead_letter_to(exchange: str, routing_key: str) -> QuorumQueueArgs:
    return {
        "x-dead-letter-exchange": exchange,
        "x-dead-letter-routing-key": routing_key,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }


def payments_new_queue(delivery_limit: int) -> RabbitQueue:
    arguments = _dead_letter_to(DEAD_LETTER_EXCHANGE_NAME, DEAD_LETTER_ROUTING_KEY)
    arguments["x-delivery-limit"] = delivery_limit
    return RabbitQueue(
        PAYMENTS_NEW_QUEUE_NAME,
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=PAYMENTS_NEW_ROUTING_KEY,
        arguments=arguments,
    )


def dead_letter_queue() -> RabbitQueue:
    return RabbitQueue(
        DEAD_LETTER_QUEUE_NAME,
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=DEAD_LETTER_ROUTING_KEY,
    )


def retry_queue_name(delay_ms: int) -> str:
    return f"{PAYMENTS_NEW_QUEUE_NAME}.retry.{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    name = retry_queue_name(delay_ms)
    arguments = _dead_letter_to(PAYMENTS_EXCHANGE_NAME, PAYMENTS_NEW_ROUTING_KEY)
    arguments["x-message-ttl"] = delay_ms
    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=name,
        arguments=arguments,
    )


async def declare_topology(
    broker: RabbitBroker, *, delivery_limit: int, retry_delays_ms: list[int]
) -> None:
    exchanges: dict[str, RobustExchange] = {}
    for exchange in (PAYMENTS_EXCHANGE, RETRY_EXCHANGE, DEAD_LETTER_EXCHANGE):
        exchanges[exchange.name] = await broker.declare_exchange(exchange)

    bindings: list[tuple[RabbitQueue, str]] = [
        (payments_new_queue(delivery_limit), PAYMENTS_EXCHANGE_NAME),
        (dead_letter_queue(), DEAD_LETTER_EXCHANGE_NAME),
        *((retry_queue(delay), RETRY_EXCHANGE_NAME) for delay in retry_delays_ms),
    ]
    for queue, exchange_name in bindings:
        declared = await broker.declare_queue(queue)
        await declared.bind(exchanges[exchange_name], routing_key=queue.routing_key)
