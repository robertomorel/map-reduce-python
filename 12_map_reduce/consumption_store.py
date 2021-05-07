import redis
import json

class ConsumptionStore(object):
  def __init__(self, host, port, db):
    self.host = host
    self.port = port
    self.db = db

    self.MODEL_PREFIX = 'br::uni7::consumption::product::{0}'
    self.connection = redis.Redis(host=self.host, port=self.port, db=self.db)

  def save_consumption(self, key, consumptions):
    self.connection.set(self.MODEL_PREFIX.format(key), consumptions)  

  def get_consumption(self, key):
    result = '[]'

    consumptions = self.connection.get(self.MODEL_PREFIX.format(key))
    if consumptions:
      result = consumptions

    result = json.loads(result)

    return result    
