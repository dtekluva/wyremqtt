from celery import Celery
from time import sleep
app = Celery('tasks', broker = 'amqps://shuzgizc:Cyn1AqSowjmaU3KWIhF_6heAERl-FHJN@fish.rmq.cloudamqp.com/shuzgizc', backend='db+postgresql://postgres:19sedimat54@localhost/postgres')
CELERY_IMPORTS = [
    'CELERY.tasks',
]