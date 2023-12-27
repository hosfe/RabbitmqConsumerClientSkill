'''
Title: RabbitmqConsumerClientSkill
Authors: Felix Hosner - fhosner@computer-coach.ch
Date: 27.12.2023

Description:
This opsdroid skill connects to RabbitMQ, consumes messages from a
queue, and processes them efficiently and reliably. – 49fda

Command:

#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨
Important developer information:
- The module is automatically recognized as a startup component of Opsdroid and initialized accordingly."
Informatione: https://docs.opsdroid.dev/en/stable/skills/events.html#opsdroidstarted

- The parameters defined in the 'configuration.yaml': 
For unknown reasons it is not possible to load them during script initialization

- Even if the parameters are not needed, it is necessary to pass them. 
For example in the function: def on_opsdroid_started(self, config, event):

The source in the development environment is stored in the directory:
./opsdroid/matrix/module

The components of the Docker instance are in this directory:
/home/opsdroid/module/rabbitmq_consumer_client_skill
'''

#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨
# Import the necessary modules and components required for the project. – 4ac06
from opsdroid.skill import Skill
from opsdroid.matchers import match_event
from opsdroid.events import OpsdroidStarted, Message
from rabbitmq_message_event.rabbitmq_message_event import RabbitMQMessageEvent
import aio_pika
import logging
import asyncio
import json

#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨
# Configuring logging is on – 5c677
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


#¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨¨
class RabbitmqConsumerClientSkill(Skill):

    def __init__(self, opsdroid, config):
        super(RabbitmqConsumerClientSkill, self).__init__(opsdroid, config)
        logger.info(f'Initialize the RabbitmqConsumerClientSkill. – 04654')

    @match_event(OpsdroidStarted)
    async def on_opsdroid_started(self, config, event):
        logger.info(f'Opsdroid started and the RabbitMQ connection was initialized successfully. – d9965')
        
        await asyncio.sleep(10)
        logger.warning(f'The start of the RabbitMQ Connector is delayed! – 44ee2')

        # Execute extraction of the configuration parameters – f3c17
        connection_params = config.get("connection_params", {})
        queue_name = config.get("queue_name")
        dlq_exchange = config.get("dlq_exchange")
        dlq_routing_key = config.get("dlq_routing_key")

        # Connect to RabbitMQ: Connect to – 6be54
        try:
            logger.info(f'Starting and initializing RabbitMQ and the associated queue. – 924b1')
            
            '''
            Important developers ionformation: 
            The parameters defined in the 'configuration.yaml' are loaded at this point. For 
            unknown reasons it is not possible to load them during script initialization
            – 6c4ee
            '''
            self.connection = await aio_pika.connect_robust(**connection_params)
            self.channel = await self.connection.channel()
            self.queue = await self.channel.declare_queue(
                queue_name, 
                durable=True,
                arguments={

                    "x-dead-letter-exchange": dlq_exchange,
                    "x-dead-letter-routing-key": dlq_routing_key
                }
            )
                       
            logger.info(f'Connected to RabbitMQ and a queue declared. – 86cb9')


            # Start receiving messages. – 7a20e
            logger.info(f'Start the RabbitMQ Consumer. – e7610')
            
            if not self.channel:
                logger.error(f'RabbitMQ Channel is not initialized. Please check whether all necessary – 8f1a5')
                return
            
            logger.info(f'The structure and declaration of a consuming function. – 05ebe')
            async with self.queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        try:
                            logger.info(f'Processing RabbitMQ messages as raw data. – 2fc05')
                            # Decode the message content as JSON format – 0c634
                            message_data = json.loads(message.body.decode())
                            
                            await self.parse(RabbitMQMessageEvent(message_data)) 
                            
                        except json.JSONDecodeError:
                            logger.error('There was an error parsing the message as JSON. The parser was able to find the N – 0d2d5', exc_info=True)

        except Exception as e:
            logger.error(f'Error connecting to RabbitMQ: {e}: An error occurred. Please – 2edc5')

    '''
    When Opsdroid shuts down, the script exits. 
    All running processes has to finish. – a9ec6
    '''
    async def disconnect(self):
        if self.channel:
            logger.info(f'Closing the RabbitMQ channel. – 8043b')
            await self.channel.close()

        if self.connection:
            logger.info(f'Complete the RabbitMQ connection. – c1bfa')
            await self.connection.close()
