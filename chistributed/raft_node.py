import json
import time
import sys
import signal
import zmq
from enum import Enum
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

election_timeout = 0.5

class Node:
  def __init__(self, node_name, pub_endpoint, router_endpoint, spammer, peer_names):
    self.loop = ioloop.ZMQIOLoop.instance()
    self.context = zmq.Context()

    self.connected = False

    # SUB socket for receiving messages from the broker
    self.sub_sock = self.context.socket(zmq.SUB)
    self.sub_sock.connect(pub_endpoint)
    # make sure we get messages meant for us!
    self.sub_sock.set(zmq.SUBSCRIBE, node_name)
    self.sub = zmqstream.ZMQStream(self.sub_sock, self.loop)
    self.sub.on_recv(self.handle)

    # REQ socket for sending messages to the broker
    self.req_sock = self.context.socket(zmq.REQ)
    self.req_sock.connect(router_endpoint)
    self.req = zmqstream.ZMQStream(self.req_sock, self.loop)
    self.req.on_recv(self.handle_broker_message)

    self.name = node_name
    self.spammer = spammer
    self.peer_names = peer_names

    self.store = {'foo': 'bar'} #*** change appropriately
    
    #RAFT sepecific terms
    self.state = "follower"
    self.last_update = time.time()
    self.curr_term = 0
    self.voted_for = None
    self.commit_index = None #*** initial value?
    self.last_applied = None #*** initial value?
    self.next_index = None #initialize upon becoming leader
    self.match_index = None # initialize upon becoming leader

	# log code
    self.log = []
    # the log will be a list of dictionaries, with key for term (initialized at 1), and key for the command for the state machine
    self.last_log_index = 0
    self.last_log_term = None


    for sig in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP, signal.SIGQUIT]:
      signal.signal(sig, self.shutdown)

  def start(self):
    '''
    Simple manual poller, dispatching received messages and sending those in
    the message queue whenever possible.
    '''
    self.loop.start()
  
  def handle_broker_message(self, msg_frames):
    '''
    Nothing important to do here yet.
    '''
    pass

  def handle(self, msg_frames):
    assert len(msg_frames) == 3
    assert msg_frames[0] == self.name
    # Second field is the empty delimiter
    msg = json.loads(msg_frames[2])

    if msg['type'] == 'get':
      	# If node not the Leader
		# redirect client to LeaderID ( either send message to broker or forward to leader)
	pass
    elif msg['type'] == 'set':
	# If node not the Leader
		# redirect client to LeaderID ( either send message to broker or forward to leader)
	# option: send message to LeaderID, but with extra field saying 'forwarded'
	# option: send message to LeaderID, but have leader treat it as if it came from client
	#self.handle_appendEntries(msg)
      pass
    elif msg['type'] == 'hello':
      # should be the very first message we see
      if not self.connected:
        self.connected = True
        self.req.send_json({'type': 'helloResponse', 'source': self.name})
        # if we're a spammer, start spamming!
        if self.spammer:
          self.loop.add_callback(self.send_spam)
    elif msg['type'] == 'spam':
      self.req.send_json({'type': 'log', 'spam': msg})
    else:
      self.req.send_json({'type': 'log', 'debug': {'event': 'unknown', 'node': self.name}})
  def handle_peerMsg(self, msg):
    '''
    if msg term > self.term:
      self.term = term
      self.state = "follower"
      self.voted_for = None
    delegate msg to appropriate handler
    '''
    return

  def handle_requestVote(self, rv):
    if self.state == "follower":
      '''
      if term < self.term:
        send reply of false
        return
      if (self.voted_for == None or self.voted == self) && rv.log more up to date than self.log):
        send reply of true
        self.last_update = time.time()
        return
      send reply of false
      return
      '''
      pass
    if self.state == "candidate" or self.state == "leader":
      '''
      send reply of false
        return
      '''
    return

  def handle_requestVoteReply(self, rvr):
    '''
    if leader:
      ignore
      return
    if follower:
      ignore
    if candidate:
      if success:
        add to accepted
      else: #failure
        add to refused
      if have qorum:
        call function for beginning leadership
    '''
    return

  def handle_appendEntries(self, ae):
    '''
    if ae_msg term < self.term: #reject
      return
    if leader:
      should never happen... (i.e. two leaders w/ same term)
      return?
        self.state == "follower"
    		# if ( msg['term'] < self.curr_term )
    			# send a response with 'yes' = false
    			# break
    		# if ( msg != {} ):
    			#if (msg['leaderCommit'] != self.commit_index)
    				# self.commit_index = min( msg['leaderCommit'], len (self.log) - 1)
    			#if ( len(self.log) < msg['prevLogIndex'] )
    				# send a response with 'yes' = false
    				# break
    			#if ( len(self.log) > 0 and self.log[msg['prevLogIndex']]['term'] != msg['prevLogTerm'] )
    				# self.log = log[:msg['prevLogIndex']]
    				# self.last_log_index = msg['prevLogIndex']
    				# self.last_log_term = msg['prevLogTerm']
    				# send a response with 'yes' = false
    				# break
    			# else
    				# if ( len(self.log) > 0 and msg['leaderCommit'] > 0 and log[msg['leaderCommit']]['term'] != msg['term'] )
    					# self.log = self.log[:self.commit_index]
    					# for e in msg['entries']:
    						# self.log.append(e)
    						# self.commit_index += 1
    					# tbcontinued
    '''
    return

  def handle_appendEntriesReply(self, aer):
    '''
    if leader:
      if success:
      else #failure:
    FILL IN
    '''
    return

  def housekeeping(self): #handles election BS
    now = time.time()
    if self.state == "follower" && now - self.last_update > election_timeout: #case of no heartbeats
      ##call election

    return
  
  def call_election(self):
    '''
    increment term
    transition to candidate
    vote for itself
    issue request_vote RPC to peers
    follow-up ***
    '''
    return

  def broadcast_heartbeat(self):
    '''
    for peer in peers
      send heartbeat to peer
    '''
    return


  def send_spam(self):
    '''
    Periodically send spam, with a counter to see which are dropped.
    '''
    if not hasattr(self, 'spam_count'):
      self.spam_count = 0
    self.spam_count += 1
    t = self.loop.time()
    self.req.send_json({'type': 'spam', 'id': self.spam_count, 'timestamp': t, 'source': self.name, 'destination': self.peer_names, 'value': 42})
    self.loop.add_timeout(t + 1, self.send_spam)

  def shutdown(self, sig, frame):
    self.loop.stop()
    self.sub_sock.close()
    self.req_sock.close()
    sys.exit(0)

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser()
  parser.add_argument('--pub-endpoint',
      dest='pub_endpoint', type=str,
      default='tcp://127.0.0.1:23310')
  parser.add_argument('--router-endpoint',
      dest='router_endpoint', type=str,
      default='tcp://127.0.0.1:23311')
  parser.add_argument('--node-name',
      dest='node_name', type=str,
      default='test_node')
  parser.add_argument('--spammer',
      dest='spammer', action='store_true')
  parser.set_defaults(spammer=False)
  parser.add_argument('--peer-names',
      dest='peer_names', type=str,
      default='')
  args = parser.parse_args()
  args.peer_names = args.peer_names.split(',')

  Node(args.node_name, args.pub_endpoint, args.router_endpoint, args.spammer, args.peer_names).start()
