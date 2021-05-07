from flask import Flask
from flask import jsonify
from recommendation_store import RecommendationStore

import json

app = Flask(__name__)

redis = RecommendationStore('localhost', '6379', 0)

@app.route('/v1/recommendation/products/<id>')
def get_recommendation(id):
  result = redis.get_recommendation(id)

  return jsonify(key=id, result=result)

if __name__ == "__main__":
  app.run()