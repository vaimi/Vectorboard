# For Rest API
from flask import Flask, request
from flask.ext.restful import Resource, Api
from flask.ext.cors import CORS

# For Websocket
import tornado.escape
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
import os.path
import os

# Threading
import threading, thread

# Heartbeat
import time

# System tools (exit)
import sys

# Node class
from node import Node

# Command line arguments
import argparse

# Logging
import logging

# Host detection.
import socket

# Random generation
import random
import string

# Time between heartbeats
HEARTBEAT_INTERVAL = 5
# Amount of time allowed to miss
HEARTBEAT_TOLERANCE = 11

# Rest api
rest_server = Flask(__name__)
api = Api(rest_server)

# The threading approach and architecture is influenced https://github.com/mishunika/MIE-DSV/
class Rest(threading.Thread):
    """
    Thread for Rest API
    """
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            try:
                if node.secure:
                    context = (os.path.join(os.path.dirname(__file__), "../cert.crt"), os.path.join(os.path.dirname(__file__), "../key.key"))
                    rest_server.run(host='0.0.0.0', port=node.port, ssl_context=context)
                else:
                    rest_server.run(host='0.0.0.0', port=node.port)
                CORS(rest_server)
            except socket.error, e:
                continue


class QueueHandler(threading.Thread):
    """
    Thread for outward message sending
    """
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global node
        while True:
            task = node.queue.get()
            if task['method']:
                getattr(node, task['method'])(*task['args'])
            try:
                node.queue.task_done()
            except ValueError:
                continue



class Heartbeat(threading.Thread):
    """
    Thread for heartbeat
    """
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        global node
        while True:
            try:
                if node.get_follower() is None:
                    continue
                node.heartbeat()
                # Make sure we get heartbeats from previous host
                if node.last_heartbeat is None:
                    node.last_heartbeat = int(time.time())
                if int(time.time()) - node.last_heartbeat > HEARTBEAT_TOLERANCE:
                    node.panic(node.host)

                time.sleep(HEARTBEAT_INTERVAL)
            except socket.error, e:
                continue

class DistributionConnection(Resource):
    """
    Connection resource for ring API. Offers functions to join and leave the ring.
    """
    def get(self):
        """
        Join the ring
        :return: int 400 if malformed request, or 200 with {host: node_old_follower_address}.
        """

        # Check that the message is not malformed
        host = request.args.get('host')
        if host == '' or host is None:
            return '', 400

        # Get old follower
        old_follower = node.get_follower()
        # For initial ring creation
        if old_follower is None:
            old_follower = node.host
        # Set joining host as follower
        node.set_follower(host)

        # Send connection message to websocket
        msg = {"method": "connected", "message": node.get_follower()}
        VectorSocketHandler.update_cache(msg)
        VectorSocketHandler.send_updates(msg)

        logging.info("API: %s is being merged to ring", host)
        return {'host': old_follower}, 200

    def post(self):
        """
        leave the ring. Requires json message {'host':leaving host, 'new_host': replacing host}
        :return: 400 if malformed request. 200 on success.
        """
        data = request.json
        leaving_host = data['host']
        replacing_host = data['new_host']
        if leaving_host is None or replacing_host is None:
            return '', 400

        logging.info("API: %s wants to leave", leaving_host)
        if node.host == leaving_host: # Message gone already through the ring
            logging.info("API: Message gone through the ring. Ready to leave.")
            msg = {'method':'disconnected', 'message':node.get_follower()}
            VectorSocketHandler.update_cache(msg)
            VectorSocketHandler.send_updates(msg)
            node.reset_node()

            return '', 200
        elif node.get_follower() != leaving_host: # Someone on the middle of the ring
            logging.info("API:Unknown node, propagating.")
            node.queue.put({'method': 'disconnect', 'args': (leaving_host, replacing_host)})
            return '', 200
        elif node.get_follower() == leaving_host: # Node that lost it's follower.
            logging.info("API: Follower left. Chancing follower to %s", replacing_host)
            if replacing_host != node.host: # If we are still a ring
                node.queue.put({'method': 'disconnect', 'args': (leaving_host, replacing_host)})
                node.propagate_message({'method': 'control', 'message': leaving_host + ' leaves.'})
                node.set_follower(replacing_host)
                node.leader_election("election", 0)
                node.queue.put({'method': 'leader_election', 'args': ("election", 0)})
            else:
                node.set_follower(None)
            return '', 200
        else:
            return '', 200


class DistributionMessages(Resource):
    """
    Message resources. Client can request and send messages through the channel.
    """

    def get(self):
        """
        Request messages in the channel
        :return: dict in Node and 200
        """
        return node.messages, 200

    def post(self):
        """
        Send messages through the channel. Format is always {'method':some_method, 'message':some message}. Message
        can be also another dictionary if required
        :return: 200 on success, 400 on malformed request
        """

        data = request.json
        command = data['method']
        message = data['message']
        logging.debug("API: Received message %r", data)
        if command is None or message is None:
            return '', 400
        if command == "election" or command == "elected":  # {'method':'election' or 'elected', 'message': UID}
            node.queue.put({'method': 'leader_election', 'args': (command, message)})
        elif command == "propagate":  # {'method':'propagate', 'message': message}
            node.queue.put({'method': 'propagate_message', 'args': (message,)})
        elif command == "persistent": # {'method':'persistent', 'message': message}
            if "method" in message:
                if message["method"] == "clean":  # {'method':'persistent', 'message':
                    # 'id': 23, method':'clean', 'message': {'x': 500, 'y': 500}}
                    node.messages.clean()
            VectorSocketHandler.update_cache(message['message'])
            VectorSocketHandler.send_updates(message['message'])
            if not node.is_leader():
                node.queue.put({'method': 'persist_message', 'args': (message["message"], message["id"])})
        else:
            logging.warning("API: Unknown message: %r", data)
        return '', 200

class DistributionHeartbeat(Resource):
    """
    Heartbeat resource. When in ring, node has to get update from previous node inside HEARTBEAT_TOLERANCE.
    If it's not getting updates, it will panic.
    """
    def post(self):
        """
        listen heartbeats. If host is set, ring is already panicking.
        :return: 200 on success.
        """
        data = request.json
        if data is not None:
            if "host" in data:
                if data['host'] != node.host:
                    node.panic(data['host'])
        else:
            node.last_heartbeat = int(time.time())
        return '', 200

# Declare API resources
api.add_resource(DistributionConnection, '/api/connection/',
                 endpoint='connection')
api.add_resource(DistributionMessages, '/api/messages/',
                    endpoint='messages')
api.add_resource(DistributionHeartbeat, '/api/heartbeat/',
                 endpoint="heartbeat")


# The tornado architecture is influenced by https://github.com/tornadoweb/tornado/tree/master/demos/chat
class Vectors(tornado.web.Application):
    """
    Main Tornado app. Sets settings ready for other parts.
    """
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/vectorsocket", VectorSocketHandler),
        ]

        settings = dict(
            cookie_secret=''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(32)),
            template_path=os.path.join(os.path.dirname(__file__), "../vectorsite/templates"),
            static_path=os.path.join(os.path.dirname(__file__), "../vectorsite/static"),
            xsrf_cookies=True,
        )
        super(Vectors, self).__init__(handlers, **settings)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        """
        Return main page
        :return: main page in html
        """
        self.render("index.html", messages=VectorSocketHandler.cache)


class VectorSocketHandler(tornado.websocket.WebSocketHandler):
    """
    Vector websocket. Handles messaging between node and client.
    """
    waiters = set()
    cache = []
    cache_size = 100
    global node

    def get_compression_options(self):
        # Non-None enables compression with default options.
        return {}

    def open(self):
        """
        On client join.
        :return:
        """
        VectorSocketHandler.waiters.add(self)

        # Send update about connection status
        if (node.get_follower() is not None):
            msg = {"method": "connected", "message": node.get_follower()}
            self.write_message(msg)

        # Write old messages
        for key, value in node.messages.iteritems():
            self.write_message({'method':value['method'], 'message':value['message']})

    def on_close(self):
        VectorSocketHandler.waiters.remove(self)

    @classmethod
    def update_cache(cls, chat):
        cls.cache.append(chat)
        if len(cls.cache) > cls.cache_size:
            cls.cache = cls.cache[-cls.cache_size:]

    @classmethod
    def send_updates(cls, message):
        """
        Send updates to all node clients.
        :param message: message to be sent
        :return:
        """
        logging.info("WS: Sending message to %d waiters", len(cls.waiters))
        for waiter in cls.waiters:
            try:
                waiter.write_message(message)
            except:
                logging.error("WS: Error sending message", exc_info=True)

    def on_message(self, json_message):
        """
        On message from the clients.
        :param json_message: json message grom the client.
        :return:
        """
        logging.info("WS: Got message %r", json_message)
        parsed = tornado.escape.json_decode(json_message)
        method = parsed['method']

        if method == "line":
            message = {
                "message": parsed["message"],
                "method": "line"
            }
            node.queue.put({'method': 'propagate_message', 'args': (message, )})

        elif method == "clean":
            message = {
                "message": parsed["message"],
                "method": "clean"
            }
            node.queue.put({'method': 'propagate_message', 'args': (message, )})

        elif method == "connect":
            target = parsed["message"]
            if target is None:
                return
            success = node.connect(target)
            if success:
                # Clean up the whiteboard
                msg = {"method": "clean", "message": {'x':500, 'y':500}}
                VectorSocketHandler.update_cache(msg)
                VectorSocketHandler.send_updates(msg)

                # Send connection message to node clients.
                msg = {"method": "connected", "message": node.get_follower()}
                VectorSocketHandler.update_cache(msg)
                VectorSocketHandler.send_updates(msg)

                # get messages from node follower
                node.request_messages()

                for key, value in node.messages.iteritems():
                    VectorSocketHandler.update_cache(value)
                    VectorSocketHandler.send_updates(value)
                # Elect new leader
                node.queue.put({'method': 'leader_election', 'args': ("election", 0)})

        elif method == "disconnect":
            response = node.disconnect(node.host, node.get_follower())

            # Message followers
            msg = {"method": "disconnected", "message": node.get_follower()}
        else:
            return


# http://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib/166520#166520
def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 0))
        IP = s.getsockname()[0]
    except:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP

if __name__ == '__main__':
    # Command line arguments
    parser = argparse.ArgumentParser(description='Distributed whiteboard.')
    parser.add_argument('-rp', '--restport', type=int, default=5000,
                        help='Port used for rest api')
    parser.add_argument('-sp', '--socketport', type=int, default=5001,
                        help='Port used for websocket')
    parser.add_argument('--host', default=get_ip(),
                        help='Set hostname')
    parser.add_argument('--ssl', action="store_true")

    args = parser.parse_args()

    # Logging settings
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    prefix = ""
    if args.ssl:
        prefix = 'https://'
    else:
        prefix = 'http://'
    node = Node(host=prefix + args.host + ":" + str(args.restport), port=args.restport)
    node.secure = args.ssl
    logging.info("The client is hosted on %s:%s", args.host, args.socketport)

    # Setup threading
    handler = QueueHandler()
    apiThread = Rest()
    heartbeat = Heartbeat()

    handler.daemon = True
    handler.start()
    heartbeat.daemon = True
    heartbeat.start()
    apiThread.daemon = True
    apiThread.start()

    # Setup Tornado
    app = Vectors()
    if node.secure:
        app.listen(args.socketport, ssl_options={
                "certfile": os.path.join(os.path.dirname(__file__), "../cert.crt"),
                "keyfile": os.path.join(os.path.dirname(__file__), "../key.key")
            })
    else:
        app.listen(args.socketport)
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        node.disconnect(node.host, node.get_follower())
        sys.exit()

