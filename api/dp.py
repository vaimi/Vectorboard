from flask import Flask, request, Response, g, jsonify, _request_ctx_stack, redirect
from flask.ext.restful import Resource, Api, abort
from flask.ext.cors import CORS
import threading
import requests as ring
import time, sys
import socket
import json

from node import Node

import abc
import argparse
import logging

server = Flask(__name__)
api = Api(server)

class Server(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        """server.run(port=node.port)
        CORS(server)"""


class Tasker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global node
        while True:
            task = node.queue.get()
            if task['method']:
                getattr(node, task['method'])(*task['args'])
            node.queue.task_done()

class DistributionConnection(Resource):
    def get(self):
        node.set_host("http://" + request.host)

        host = request.args.get('host')
        if host == '' or host is None:
            return '', 400
        if node.get_follower() is None:
            node.set_follower(host)
            node.queue.put({'method': 'propagate_message', 'args': ({'method': 'control', 'message': host + ' joins.'},)})
        logging.info(host + " is trying to the ring")
        return '', 200

    def post(self):
        node.set_host("http://" + request.host)

        dataDict = request.json
        target = dataDict['host']
        new_target = dataDict['new_host']
        logging.info(target + " wants to leave")
        if node.host == target:
            logging.info("I'm the host leaving, I'm ready to quit")
            return '', 200
        elif node.get_follower() != target and new_target != '':
            logging.info("I don't know who that is, propagating.")
            node.queue.put({'method': 'disconnect', 'args': (target, new_target)})
            return '', 200
        elif node.get_follower() == target:
            logging.info("Oh, so sad. Chancing follower to " + new_target)
            if new_target != node.host:
                node.queue.put({'method': 'disconnect', 'args': (target, '')})
                node.set_follower(new_target)
                node.queue.put(
                    {'method': 'propagate_message', 'args': ({'method': 'control', 'message': target + ' leaves.'},)})
                node.queue.put({'method': 'leader_election', 'args': ("election", 0)})
            else:
                node.set_follower(None)
            return '', 200
        else:
            return '', 200

class DistributionMessages(Resource):

    def get(self):
        node.set_host("http://" + request.host)
        host = request.remote_addr
        logging.info(node.leader + " is requesting messages")
        return '', 200

    def post(self):
        node.set_host("http://" + request.host)
        dataDict = request.json
        host = request.remote_addr
        logging.info(host + " is trying to propagate an message")
        command = dataDict['method']
        if command == "election" or command == "elected":
            node.queue.put({'method': 'leader_election', 'args': (command, dataDict['vote'])})
        elif command == "propagate":
            node.queue.put({'method': 'propagate_message', 'args': (dataDict["message"],)})
        elif command == "persistent":
            if not node.is_leader():
                node.queue.put({'method': 'persist_message', 'args': (dataDict["message"], dataDict["id"])})
        return '', 200

class UIConnection(Resource):
    def get(self):
        node.set_host("http://" + request.host)
        target = request.args.get('host')
        if target == '' or target == None:
            return '', 400
        response = node.connect(target)
        if response == 200:
            node.set_follower(target)
            node.queue.put({'method': 'leader_election', 'args': ("election", 0)})
            return '', response
        else:
            return '', 404

    def post(self):
        node.set_host("http://" + request.host)
        if node.follower is None:
            logging.warning("You are not in a ring. You should connect first.")
            return '', 400
        node.disconnect(node.host, node.get_follower())
        logging.info("Ready to exit, cleanly disconnected from hosts")
        return '', 200

class UIMessages(Resource):
    def get(self):
        node.set_host("http://" + request.host)
        from_id = request.args.get("from_id")
        if from_id is None:
            from_id = 0
        messages = {k: v for (k, v) in node.messages.items() if int(k) >= int(from_id)}
        logging.info(node.host + " is requesting messages")
        return messages, 200
    def post(self):
        node.set_host("http://" + request.host)
        dataDict = request.json
        method = dataDict['method']
        message = dataDict['message']
        messageDict = {'method': method, 'message': message}
        logging.info(node.host + " is trying to send an message")
        node.queue.put({'method': 'propagate_message', 'args': (messageDict, )})
        return '',200

class UIMessage(Resource):
    def get(self):
        from_id = request.args.get('from_id')
        node.set_host("http://" + request.host)
        if from_id is None:
            from_id = 0
        messages = {k: v for (k, v) in node.messages.items() if int(k) >= int(from_id)}
        logging.info(node.host + " is requesting messages")
        return jsonify(messages), 200


'''
UI functions:

Resources:
Line
Canvas
Connection

Functions:

/line
add_line(point x, point y)
/line/x
get_line_status()

/canvas
new_canvas(size x, size y)
get_canvas()

/connect
disconnect()
connect()

RING:

Resources:

Message
Connection
Heartbeat
Leader

Functions:

message
send_message()
persist_message()
message/x
get_message_status()

connection
add_to_ring()
remove_from_ring()

heartbeat
listen_heartbeat()
panic()

leader
suggest_leader()
'''

tasker = Tasker()
apiThread = Server()
api.add_resource(DistributionConnection, '/vectorsite/api/distribution/connection/',
                 endpoint='d_connection')
api.add_resource(DistributionMessages, '/vectorsite/api/distribution/messages/',
                    endpoint='d_messages')
api.add_resource(UIConnection, '/vectorsite/api/ui/connection/',
                    endpoint='ui_connection')
api.add_resource(UIMessages, '/vectorsite/api/ui/messages/',
                    endpoint='ui_messages')

parser = argparse.ArgumentParser(description='Distributed whiteboard.')
parser.add_argument('-p', '--port', type=int, default=5000,
                    help='Port used for rest api')
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

node = Node(port=args.port)

tasker.daemon = True
tasker.start()

apiThread.daemon = True
apiThread.start()



if __name__ == '__main__':
    server.run(port=node.port)
    CORS(server)
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            node.disconnect(node.host, node.get_follower())
            sys.exit()
