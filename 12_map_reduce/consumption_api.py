from flask import Flask
from flask import jsonify
from consumption_store import ConsumptionStore

import json

app = Flask(__name__)

redis = ConsumptionStore('localhost', '6379', 0)

@app.route('/v1/consumption/products/<id>')
def get_consumption(id):
  result = redis.get_consumption(id)

  return jsonify(key=id, result=result)

if __name__ == "__main__":
  app.run()