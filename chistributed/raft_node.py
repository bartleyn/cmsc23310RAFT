import json
import sys
import signal
import zmq
import random
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

term_timeout = 0.5
min_election_timeout = 0.15
max_election_timeout = 0.3
polling_timeout = 0.05
heartbeat_timeout = 0.05

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
    self.last_update = self.loop.time()
    self.term = 0
    self.voted_for = None
    self.commit_index = 0 #*** initial value?
    self.last_applied = 0 #*** initial value?
    self.next_index = None
    #re-initialize upon election: dictionary mapping node namer to index of the next log entry to send to that server
    self.match_index = None
    #re-initialize upon election: dictionary mapping node names to the highest log index replicated on that server
    #other things?
    self.leaderId = None # adress of curent leader
    
    #things needed for Log Replication
    self.appendVotes = {} #dictionary mapping keys to lists of nodes that have Replied to Append
    self.logQueue = {} #dictionary in same format as log that need to be replicated

    #log code
    self.log = []
    # the log will be a list of dictionaries, with key for term (initialized at 1), and key for the command for the state machine
    self.last_log_index = 0
    self.last_log_term = 0
    self.qorum = (len(peer_names) + 1)/2 + 1

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
    if msg['type'] == 'hello':
      self.handle_hello(msg)
    elif msg['type'] == 'get':
      self.handle_get(msg)
    elif msg['type'] == 'set':
      self.handle_set(msg)      
    elif msg['type'] == 'spam':
      self.req.send_json({'type': 'log', 'spam': msg, 'this':'message'})
    else:
      self.handle_peerMsg(msg)
    return
  
  def handle_hello(self, msg):
    # should be the very first message we see
    if not self.connected:
      self.connected = True
      self.req.send_json({'type': 'helloResponse', 'source': self.name})
      self.housekeeping()
      # if we're a spammer, start spamming!
      #if self.spammer:
      #  self.loop.add_callback(self.send_spam)


  def handle_get(self, msg):
    #If node not the Leader
      #redirect client to LeaderID ( either send message to broker or forward to leader)
    #else
      #send response with the value self.store[msg[key]]
      #self.send_message('getResponse', self.name, msg['source'], True, msg['key'], self.store[msg['key']], msg['id'])
    return

  def handle_set(self,s):
   
    if self.state == "leader":
        self.logQueue[s.key] = s.value #add request to queue
        self.appendVotes[s.key] = () #make room to record replies
        self.req.send_json({"type": "setResponce", "value": s.value}) #send setResponce
        self.req.send_json({"type": 'appendEntries', "destination": peer_names, "term": self.term, "leaderId": self.name, "prevLogIndex": self.last_log_index, "prevLogTerm": last_log_term, "entries": [{s.key: s.value}], "leaderCommit": self.commit_index}) #send appendEntries messages to all folowers
    elif self.checkLeader:
      # option: send message to LeaderID, but with extra field saying 'forwarded'
      # option: send message to LeaderID, but have leader treat it as if it came from client
      # I'm going with no extra field, but since set messages are only sent by the broker, I am calling it a forwardedSet message
        self.req.send_json({"type": "forwardedSet", "destination": leaderId, "key": s.key, "value": s.value})
        self.req.send_json({"type": "setResponce", "value": s.value}) #if the leader crashes this might cause problems
    else:
        self.req.send_json({"type": "setResponce", "error": "No Leader currently exists, please wait and try again"})
    
    return


  def handle_peerMsg(self, msg):
    msg_term = msg['term']
    if msg_term > self.term:
      self.term = msg_term
      self.state = "follower"
      self.voted_for = None
    if msg['type'] == 'requestVote':
      self.handle_requestVote(msg)
    elif msg['type'] == 'appendEntries':
      self.handle_appendEntries(msg)
    elif msg['type'] == 'requestVoteReply':
      self.handle_requestVoteReply(msg)
    elif msg['type'] == 'appendEntriesReply':
      self.handle_appendEntriesReply(msg)
    else:
      self.req.send_json({'type': 'log', 'debug': {'event': 'unknown', 'node': self.name}})

  def handle_requestVote(self, rv):
    self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE', 'node': self.name}})
    if self.state == "follower":
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWER', 'node': self.name}})
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWER', 'node': self.name}})
      if rv['term'] < self.term:
        self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWER THEIR TERM LESS', 'node': self.name}})
        self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination':rv['source'], 'voteGranted': False, 'term' : self.term})
        return
      elif (self.voted_for == None or self.voted_for == self.name) and (rv['lastLogTerm'] >= self.last_log_term and rv['lastLogIndex'] >= self.last_log_index):
        self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWER WE CAN VOTE FOR THEM', 'node': self.name}})
        self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination': rv['source'], 'voteGranted': True, 'term' : self.term})
        self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE VOTE GRANTED', 'node': self.name}})
        self.last_update = self.loop.time()
      else:
        self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWE WE VOTED OR THEY HAVE INVALID LOG', 'node': self.name}})
        self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination':rv['source'], 'voteGranted': False, 'term' : self.term})
      return

    else: # self.state == "candidate" or self.state == "leader":
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE NOT FOLLOWER', 'node': self.name}})
      self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination':rv['source'], 'voteGranted': False, 'term' : self.term})
    return

  def handle_requestVoteReply(self, rvr):
    self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE REPLY', 'node': self.name}})
    if self.state == "candidate": #case candidate
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE REPLY CASE CANDIDATE', 'node': self.name}})
      if rvr['voteGranted'] == True:
        self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE REPLY CASE CANDIDATE VOTE GRANTED', 'node': self.name}})
        if (rvr['source'] not in self.accepted):
          self.accepted.append(rvr['source'])
          if len(self.accepted) >= self.qorum:
            self.begin_term()
      else:
        if rvr['source'] not in self.refused:
          self.refused.append(rvr['source'])
    #otherwise ignore
    return

  def handle_appendEntries(self, ae):
    if msg['term'] < self.term:
      self.send_message('log', msg)
      self.send_message('appendEntriesReply', self.name, msg['source'], false)
      return
    if self.state == "follower":
      self.last_update = self.loop.time()
      '''
      if (msg['leaderCommit'] != self.commit_index)
         self.commit_index = min( msg['leaderCommit'], len (self.log) - 1)
      if ( len(self.log) < msg['prevLogIndex'] )
        self.send_message('log', msg)
        self.send_message('appendEntriesReply', self.name, msg['source'], false)
        break
      if ( len(self.log) > 0 and self.log[msg['prevLogIndex']]['term'] != msg['prevLogTerm'] )
         self.log = log[:msg['prevLogIndex']]
         self.last_log_index = msg['prevLogIndex']
         self.last_log_term = msg['prevLogTerm']
         self.send_message('log', msg)
         self.send_message('appendEntriesReply', self.name, msg['source'], false)
         break
       else
        if ( len(self.log) > 0 and msg['leaderCommit'] > 0 and log[msg['leaderCommit']]['term'] != msg['term'] )
           self.log = self.log[:self.commit_index]
           for entry in msg['entries']:
             self.log.append(e)
             self.commit_index += 1
           self.last_log_index = len(log) - 1
           self.last_log_term = log[-1]['term']
           self.commit_index = len(log) -1 
          self.send_message('appendEntriesReply', self.name, msg['source'], True, msg)
          self.send_message('log', msg) 
      self.send_message('appendEntriesReply', self.name, msg['source'], True)
      self.send_message('log', msg)
      return
      '''
    else: #case leader or candidate
      return
  '''
  def send_message(self, type, msg):
  if type == 'log':
    self.req.send_json({'type': type, 'msg': msg})
  return

  def send_message(self, type, src, dst, yes, key, value, id, msg):
  self.req.send_json({'type': type, 'success': yes, 'source': src, 'dest': dst, 'key': key, 'value': value, 'id': id})
  def send_message(self, type, src='', dst='', yes=True,  msg=None ):
  self.req.send_json({'type': type, 'success': yes,  'source': src, 'dest': dst, 'term': self.term})
  return
  '''

  def handle_appendEntriesReply(self, aer):
    '''
    if leader:
      if aer.success:
        if self.appendVotes.has_key(aer.key): 
       # V is this supposed to be the source name or dest name? I'm assuming source
          if aer.name not in self.appendVotes[aer.key]: #dont allow repeat voting
            self.appendVotes[aer.key].append(aer.source)
            if len(self.appendVotes[aer.key]) = majority : #if majority followers have responded
              self.log[self.term][aer.key] = aer.value # comit value to log
    # ^ I think this line will look more like this:
        self.log.append({'term': self.term, aer.key: aer.value})
        self.last_log_index += 1
        self.last_log_term = self.term
        # and then anything else we need to update
    #send commit messages
              #should we bother removing from logqueue and appendvotes here?
      else: #not sucess
        #force follower to copy log
    else:
      print "Warning, " + self.name + "recieved appendEntriesReply while not leader"
    '''   
    return

  def housekeeping(self): #handles election BS
    now = self.loop.time()
    self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING TOP LEVEL', 'node': self.name}})
    if self.state == "follower":
      if now - self.last_update > term_timeout: #case of no heartbeats
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE FOLLOWER TERM TIMEOUT', 'node': self.name}})
        self.call_election()
        self.loop.add_timeout(min(self.election_timeout, now + polling_timeout), self.housekeeping)
      else:
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE FOLLOWER NO TIMEOUT', 'node': self.name}})
        self.loop.add_timeout(self.last_update + term_timeout, self.housekeeping)
    elif self.state == "candidate":
      if now < self.election_timeout: #case within an election but haven't won nor timeout occurred
        if len(self.refused) < self.qorum: #still chance of winning; poll more votes
          #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE CANDIDATE REPOLL', 'node': self.name}})
          self.poll()
          self.loop.add_timeout(min(self.election_timeout, now + polling_timeout), self.housekeeping)
        else: #no chance of winning election
          self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE CANDIDATE REFUSED > QORUM', 'node': self.name}})
          self.loop.add_timeout(self.election_timeout, self.housekeeping)
      else: # election timeout has occurred
        self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE CANDIDATE ELECTION TIMEOUT', 'node': self.name}})
        self.call_election()
        self.loop.add_timeout(min(self.election_timeout,now + polling_timeout), self.housekeeping)
    else: #case leader
      self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE LEADER', 'node': self.name}})
      self.broadcast_heartbeat()
      self.loop.add_timeout(heartbeat_timeout, self.housekeeping)
    return
  
  def call_election(self):
    self.req.send_json({'type': 'log', 'debug': {'event': 'CALL ELECTION', 'node': self.name}})
    self.term += 1
    self.state = "candidate"
    self.accepted = []
    self.refused = []
    self.election_timeout = self.loop.time() + random.uniform(min_election_timeout, max_election_timeout)
    self.accepted.append(self)
    self.poll()
    return
 
  def poll(self):
    self.req.send_json({'type': 'log', 'debug': {'event': 'POLL', 'node': self.name}})
    for peer in self.peer_names:
      if (peer not in self.refused) and (peer not in self.accepted):
        self.req.send_json({'type': 'requestVote', 'source': self.name, 
          'destination': peer, 'term': self.term, 'lastLogIndex': self.last_log_index, 
          'lastLogTerm': self.last_log_term})
    return

  def begin_term(self):
    self.req.send_json({'type': 'log', 'debug': {'event': 'BEGIN TERM', 'node': self.name}})
    self.state = "leader"
    self.next_index = {}
    self.match_index = {}
    #send append entries RPC to all others
    return

  def broadcast_heartbeat(self):
    for peer in peer_names:
      self.req.send_json({'type': 'appendEntries', 'source': self.name, 
          'destination': peer, 'term': self.term, 'prevLogIndex': 0, 
          'prevLogTerm': 0, 'entries': None, 'leaderCommit': self.commit_index}) #*** this needs to be changed to reflect actual AE RPCs
    return

  def apply_commits(self):
    '''
    if self.commit_index > self.last_applied:
      self.last_applied += 1
      entry = self.log[self.last_applied]
      key = entry['key']
      value = entry['value']
      self.store[key] = value   

    commit each log entry until the next commit index
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
