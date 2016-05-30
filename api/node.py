import logging
import requests as ring
import random
from Queue import Queue

class Node(object):
    def __init__(self, host, port=5000):
        """
        Node-class initializer. Handles node data-handling and outward messaging.
        """
        self.queue = Queue(10) # Outward message queue
        self.messages = {} # Message log

        self.follower = None # Follower address
        self.host = host # Own address
        self.port = port # Rest API port

        # Rest API urls
        self.connectionUrl = "/api/connection/"
        self.messageUrl = "/api/messages/"
        self.heartbeatUrl = "/api/heartbeat/"

        self.leader = None # Current leader
        self.voting = False # In-voting process
        self.uid = random.randint(1,100000000) # Identifier for voting

        self.last_heartbeat = None # Last time heartbeat was received
        self.timeout = 0.5

    def reset_node(self):
        """
        Reset node so it's ready for next connection
        :return:
        """
        self.queue.queue.clear() # Outward message queue
        self.messages = {} # Message log

        self.follower = None # Follower address

        self.leader = None # Current leader
        self.voting = False # In-voting process
        self.uid = random.randint(1,100000000) # Identifier for voting

        self.last_heartbeat = None # Last time heartbeat was received

    def set_uid(self, uid):
        """
        Set node UID
        :param uid: 
        :return: 
        """
        self.uid = uid

    def get_uid(self, uid):
        """
        Get node UID
        :param uid: int UID
        :return: 
        """
        return self.uid

    def set_follower(self, host):
        """
        Set Node follower.
        :param host: String Next node full address.
        :return: True on success
        """
        logging.debug("NODE: New follower: %s", host)
        self.follower = host
        return True

    def get_follower(self):
        """
        Get Node follower.
        :return: String Next node full address.
        """
        return self.follower

    def set_host(self, host):
        """
        Set own address.
        :param host: String Full Node address
        :return: True on success
        """
        logging.debug("NODE: New host address: %s", host)
        self.host = host
        return True

    def set_leader(self, leader_uid):
        """
        Set new leader.
        :param leader_uid: int Leader UID
        :return: True on success
        """
        self.leader = leader_uid
        return True

    def is_leader(self):
        """
        Checks whether Node UID corresponds to leader UID
        :return: True on match, False if Node is not leader
        """
        return self.leader is self.uid

    def leader_election(self, state, leader):
        """
        Leader election algorithm. Select node with biggest UID as new leader.
        :param state: String. Should be either "election" (currently voting) or "elected" (leader has been selected).
        :param leader: Int. Suggested or selected leader UID
        :return: False on problems/end, True on normal voting process.
        """
        leader = int(leader)
        # 1st step: Voting
        method = ""
        if state == "election":
            self.leader = None
            logging.debug("NODE: Voting for leader. Current leader %s", leader)
            if leader > self.uid:
                # Smaller UID. Not gonna lead
                method = "election"
                self.voting = True
                logging.debug("NODE: My uid is smaller %d. Current leader %d", self.uid, leader)
            elif leader < self.uid and not self.voting:
                # Bigger UID. Might be a leader
                method = "election"
                self.voting = True
                leader = self.uid
                logging.debug("NODE: My uid is bigger %d. Current leader %d", self.uid, leader)
            elif leader == self.uid and not self.voting:
                self.uid = random.randint(1, 100000000)
                logging.debug("NODE: Hitted same uid. Generating a new one.")
                self.leader_election("election", leader)
            elif leader == self.uid:
                # Voting is over. I got the biggest UID
                self.leader = self.uid
                self.voting = False
                method = "elected"
                logging.debug("NODE: I'm new leader. Current leader: %d", leader)
            else:
                return False
        # 2nd step: Announcement
        elif state == "elected":
            if self.uid == leader:
                # Announcement is over. Let's end spamming
                return False
            self.voting = False
            self.leader = leader
            method = "elected"
            logging.debug("NODE: Selected new leader %d", leader)

        # send the message out.
        message = {"method": method, "message": leader}
        try:
            ring.post(self.follower + self.messageUrl, json=message, timeout=self.timeout)
        except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
            logging.error("NODE: Cannot connect host %s. Unsend message: %s", self.follower, message)
            return False
        except TypeError:
            return False
        return True

    def propagate_message(self, message):
        """
        Propagate messages through the ring. The message is propagated until it reaches the leader.
        :param message: String message to be propagated.
        :return: boolean True on success, False if there is errors like missing follower.
        """
        if (not self.is_connected()):
            return False
        # If we are not leading. We have to keep propagating the message.
        if not self.is_leader():
            logging.debug("NODE: Propagating message: %s", message)
            msg = {'method':'propagate', 'message': message}
            try:
                ring.post(self.follower + self.messageUrl, json=msg, timeout=self.timeout)
            except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
                logging.error("NODE: Cannot connect host %s. Unsend message: %s", self.follower, msg)
                return False
        # We are leader. Lets persist the message
        else:
            logging.debug("NODE: Starting persisting process for message %s", message)

            # Get free id for message
            message_id = len(self.messages) + 1

            # Save message to cache.
            self.messages[message_id] = message

            # Announce
            msg = {'method': 'persistent', 'message': {'id': message_id, 'message': message}}
            try:
                ring.post(self.follower + self.messageUrl, json=msg, timeout=self.timeout)
            except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
                logging.error("NODE: Cannot connect host %s. Unsend message: %s", self.follower, message)
                return False
        return True

    def persist_message(self, message, message_id):
        """
        Save persistent messages to Node cache and propagate to others.
        :param message: Message to be propagated
        :param message_id: message id decided by leader
        :return:
        """
        if not self.is_connected():
            return False
        logging.debug("NODE: Persisting message: %s", message)
        self.messages[message_id] = message

        msg = {'method': 'persistent', 'message': {'id': message_id, 'message': message}}
        try:
            ring.post(self.follower + self.messageUrl, json=msg, timeout=self.timeout)
        except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
            logging.error("NODE: Cannot connect host %s. Unsend message: %s", self.follower, message)
            return False
        return True

    def connect(self, target_node):
        """
        Connect to another Node. It can be any member of the ring.
        :param target_node: Target node full address.
        :return: True on succesful connection, False on fail
        """
        if self.follower is not None:
            logging.warning("NODE: You are already in a ring. You should disconnect first.")
            return False

        # Suggest ourselves as follower
        message_dictionary = {"host": self.host}
        try:
            r = ring.get(target_node + self.connectionUrl, message_dictionary, timeout=self.timeout)
        except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
            logging.error("NODE: Cannot connect host %s.", target_node)
            return False
        if r.status_code == 200:
            # We are accepted
            data = r.json()
            # Set the target host old follower to our follower
            self.set_follower(data['host'])
            logging.debug("NODE: connected between %s and %s", target_node, data['host'])
            return True
        else:
            return False

    def disconnect(self, leaving_node, new_target):
        """
        Disconnect the ring
        :param leaving_node: String. Address of leaving node
        :param new_target: String. Node that replaces the leaving one
        :return: True on success, False on fails.
        """
        if not self.is_connected(leaderless=True):
            return False
        if self.follower is not None:
            message = {"host": leaving_node, "new_host": new_target}
            try:
                ring.post(self.follower + self.connectionUrl, json=message, timeout=self.timeout)
            except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
                logging.error("NODE: Cannot connect host %s. Unsend message: %s", self.follower, message)
                return False
            return True

    def heartbeat(self):
        """
        Send heartbeat to the next node.
        :return: True on success, false on errors.
        """
        if (not self.is_connected(leaderless=True)):
            return False
        try:
            logging.debug("NODE: Sending heartbeat to %s", self.follower)
            ring.post(self.follower + self.heartbeatUrl, timeout=self.timeout)
            return True
        except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
            logging.error("NODE: Cannot connect host %s.", self.follower)
            return False

    def panic(self, host):
        """
        Panic if too many heartbeats are lost. If some of the host cannot be connected,
        it's replaced by the suggested node.
        :param host: address of replacing node.
        :return: True on normal run, False on errors
        """
        if (not self.is_connected(leaderless=True)):
            return False
        logging.debug("NODE: %s is panicking!", host)
        msg = {"host": host}
        try:
            ring.post(self.follower + self.heartbeatUrl, json=msg, timeout=self.timeout)
            return True
        except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
            self.set_follower(host)
            return True

    def is_connected(self, leaderless=False):
        """
        Check that we have everything allright before sending messages.
        :param leaderless: Skip leader check
        :return: True if everything is allright. False if not
        """
        if self.get_follower() is None:
            logging.warning("NODE: There is no followers. Message not sent.")
            return False
        elif self.leader is None and leaderless is False:
            logging.warning("NODE: There is no leader. Message not sent.")
            self.leader_election("election", 0)
            return False
        return True

    def request_messages(self):
        """
        Request messages from following node so the new node doesn't have to start from blank whiteboard.
        :return: True on normal run, False on fail.
        """
        if not self.is_connected(leaderless=True):
            return False
        logging.debug("NODE: Requesting old messages from %s", self.follower)
        try:
            resp = ring.get(self.follower + self.messageUrl, timeout=self.timeout)
        except ring.exceptions.ConnectionError or ring.exceptions.ReadTimeout:
            logging.error("NODE: Cannot connect host %s.", self.follower)
            return False
        self.messages = resp.json()
        return True


