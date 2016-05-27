import logging
import requests as ring
import random
import sys
from Queue import Queue

class Node(object):
    def __init__(self, port):
        self.port = port
        self.queue = Queue(10)
        self.messages = {}
        self.follower = None
        self.host = None
        self.connectionUrl = "/vectorsite/api/distribution/connection/"
        self.messageUrl = "/vectorsite/api/distribution/messages/"
        self.leader = None
        self.voting = False
        self.uid = random.randint(1,10000000)

    def reset_node(self):
        self.queue = Queue(10)
        self.messages = {}
        self.follower = None
        self.leader = None
        self.voting = False

    def uid_to_host(self, uid):
        return reduce(lambda a, b: a << 8 | b, map(int, uid.split(".")))

    def host_to_uid(self, host):
        return ".".join(map(lambda n: str(host >> n & 0xFF), [24, 16, 8, 0]))

    def set_follower(self, host):
        logging.debug("Setting follower to " + str(host))
        self.follower = host

    def get_follower(self):
        return self.follower

    def set_uid(self, uid):
        self.uid = uid

    def get_uid(self, uid):
        return self.uid

    def set_host(self, host):
        self.host = host

    def set_leader(self, is_leader):
        self.leader = is_leader

    def is_leader(self):
        return self.leader == self.uid

    def leader_election(self, method, leader):
        if self.follower is None:
            self.leader = None
            return
        leader = int(leader)
        if method == "election":
            logging.debug("Voting for leader. Current leader " + str(leader))
            if leader > self.uid:
                self.voting = True
                logging.debug("My uid is smaller (" + str(self.uid) + "). Current leader " + str(leader))
            elif leader < self.uid and not self.voting:
                self.voting = True
                leader = self.uid
                logging.debug("My uid is bigger (" + str(self.uid) + "). Current leader " + str(leader))
            elif leader == self.uid:
                self.voting = False
                self.leader = self.uid
                method = "elected"
                logging.debug("I'm new leader, SEE: Current leader " + str(leader))
        elif method == "elected":
            if self.leader == leader:
                return
            self.voting = False
            self.leader = leader
            logging.debug("Selected new leader " + str(leader))
        message = {"method": method, "vote": leader}
        ring.post(self.follower + self.messageUrl, json=message)

    def propagate_message(self, message):
        if self.follower is None:
            logging.warning("There is no followers. Message not sent.")
            return False
        if not self.is_leader():
            msg = {'method':'propagate', 'message': message}
            ring.post(self.follower + self.messageUrl, json=msg)
        else:
            logging.debug("I'm leader, let's persist this")
            message_id = len(self.messages) + 1
            self.messages[message_id] = message
            msg = {'method':'persistent', 'id': message_id, 'message': message}
            ring.post(self.follower + self.messageUrl, json=msg)
        return True

    def persist_message(self, message, message_id):
        if self.follower is not None:
            logging.debug("Persisting " + str(message_id) + " message " + str(message))
            self.messages[message_id] = message
            msg = {'method': 'persistent', 'id': message_id, 'message': message}
            ring.post(self.follower + self.messageUrl, json=msg)

    def connect(self, target_node, join=False):
        if self.follower is not None and join == False:
            logging.warning("You are already in a ring. You should disconnect first.")
            return 404
        message_dictionary = {"host": self.host}
        r = ring.get(target_node + self.connectionUrl, message_dictionary)
        if r.status_code == 200:
            return 200
        else:
            return 404

    def disconnect(self, leaving_node, new_target):
        if self.follower is not None:
            message_dictionary = {"host": leaving_node, "new_host": new_target}
            ring.post(self.follower + self.connectionUrl, json=message_dictionary)

