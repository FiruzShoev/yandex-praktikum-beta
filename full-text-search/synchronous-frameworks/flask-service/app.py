from flask import Flask
from flask import request

app = Flask(__name__)


@app.route('/client/info')
def client_info():
    return {'user_agent': request.user_agent.string}
