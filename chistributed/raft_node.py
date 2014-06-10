import json
import sys
import signal
import zmq
import random
import time
from zmq.eventloop import ioloop, zmqstream
ioloop.install()

term_timeout = 0.5
min_election_timeout = 0.15
max_election_timeout = 0.3
polling_timeout = 0.05
heartbeat_timeout = 0.05
commit_timeout = 0.05

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

    self.store = {}
    
    #RAFT sepecific terms
    self.state = "follower"
    self.last_update = self.loop.time()
    self.term = 0
    self.voted_for = None
    self.commit_index = -1 #initialized to -1 because first index in log is 0
    self.last_applied = -1 #initialized to -1 because first index in log is 0
    self.next_index = None #re-initialize upon election: dictionary mapping node names to index of the next log entry to send to that server
    self.match_index = None #re-initialize upon election: dictionary mapping node names to the highest log index replicated on that server
    self.leaderId = None # adress of curent leader
    self.election_timeout = self.loop.time() + random.uniform(min_election_timeout, max_election_timeout)
    self.pending_sets = []
    
    '''
    #things needed for Log Replication
    #self.appendVotes = {} #dictionary mapping keys to lists of nodes that have Replied to Append; taken care of by match_index
    #self.logQueue = {} #dictionary in same format as log that need to be replicated
    '''
    #log code
    self.log = []
    # the log will be a list of dictionaries, with key for term (initialized at 1), and key for the command for the state machine
    self.last_log_index = -1 #initialized to -1 because first index in log is 0
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
      self.loop.add_callback(self.housekeeping) #NOTE: I believe this is threadsafe but am not certain; be wary of race conditions 
      self.loop.add_callback(self.apply_commits)
      # if we're a spammer, start spamming!
      #if self.spammer:
      #  self.loop.add_callback(self.send_spam)

  def handle_get(self, msg):
    self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE GET', 'node': self.name}})
    #If node not the Leader
      #redirect client to LeaderID ( either send message to broker or forward to leader)
    #else
      #send response with the value self.store[msg[key]]
      #self.send_message('getResponse', self.name, msg['source'], True, msg['key'], self.store[msg['key']], msg['id'])
    self.req.send_json({'type': 'getResponse', 'id': msg['id'], 'value': 0})
    return
  
  def handle_set(self,msg):
    self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE SET', 'node': self.name, 'state': self.state}})
    self.pending_sets[msg['ID']] = msg
     
    '''
    if self.state == "leader":
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE SET REQUEST AS LEADER', 'node': self.name}})
      self.log.append({'key': msg['key'], 'value': msg['value'], 'term': self.term})
      self.next_index[self.name] = len(self.log)
      
      
      self.logQueue[msg['key']] = msg['value'] #add request to queue
      self.appendVotes[msg['key']] = [] #make room to record replies
      self.appendVotes[msg['key']].append(self.name) #add leader's vote
      if not forwarded:
        self.req.send_json({'type': 'log', 'debug': {'event': 'SEND SET RESPONSE', 'node': self.name}})
        self.req.send_json({'type': "setResponse", 'id': msg['id'], 'value': msg['value']}) #send setResponse # <- this should be done after commiting
      self.req.send_json({'type': 'appendEntries', 'destination': self.peer_names,
                          'term': self.term, 'source': self.name, 'prevLogIndex': self.last_log_index,
                          'prevLogTerm': self.last_log_term, 'entries':
                            [{'key': msg['key'], 'value': msg['value'],
                              'term': self.term}], 'leaderCommit': self.commit_index}) #send appendEntries messages to all folowers

    elif self.leaderId:
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE SET REQUEST IF THERE IS LEADER', 'node': self.name}})
      self.req.send_json({'type': 'forwardedSet', 'destination': self.leaderId, 'key': msg['key'], 'value': msg['value'], 'term': self.term})

      if not forwarded: #just incase leader changes require more than one forwarding
        self.req.send_json({'type': 'setResponse', 'id': msg['id'], 'value': msg['value']}) #if the leader crashes this might cause problems

    else:
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE SET REQUEST WITH NO LEADER', 'node': self.name, 'state': self.state}})
      self.pending_sets.append(msg)
      #I'm thinking it might be better to block here until a leader has been elected.
      if self.state == "leader" and forwarded:
    self.req.send_json({'type': 'receivedFwdSetReq', 'source': self.name, 'destination': msg['source'], 'term': self.term, 
        'id': msg['id'], 'key': msg['key'], 'value': msg['value']})
    '''
    return


  def handle_peerMsg(self, msg):
    msg_term = msg['term']
    if msg_term > self.term:
      self.term = msg_term
      self.state = "follower"
      self.voted_for = None
    if msg['type'] == 'forwardedSet':
      self.handle_fwdSet(msg)
    elif msg['type'] == 'forwardedSetReply':
      self.handle_fwdSetReply(msg)
    elif msg['type'] == 'requestVote':
      self.handle_requestVote(msg)
    elif msg['type'] == 'appendEntries':
      self.handle_appendEntries(msg)
    elif msg['type'] == 'requestVoteReply':
      self.handle_requestVoteReply(msg)
    elif msg['type'] == 'appendEntriesReply':
      self.handle_appendEntriesReply(msg)
    else:
      self.req.send_json({'type': 'log', 'debug': {'event': 'unknown', 'node': self.name}})

  def handle_fwdSetReply(self, msg):
    self.pending_sets.pop(msg['ID'])
    return

  def handle_fwdSet(self,msg):
    self.pending_sets[msg['ID']] = msg
    self.req.send_json({'type': 'forwardedSetReply', 'destination': msg['source'],'term': self.term, 'ID': msg['setRequest']['ID']})

  def handle_requestVote(self, rv):
    self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE', 'node': self.name}})
    if self.state == "follower":
      #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWER', 'node': self.name}})
      if rv['term'] < self.term:
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWER THEIR TERM LESS', 'node': self.name}})
        self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination':rv['source'], 'voteGranted': False, 'term' : self.term})
        return
      elif (self.voted_for == None or self.voted_for == self.name) and (rv['lastLogTerm'] >= self.last_log_term and rv['lastLogIndex'] >= self.last_log_index):
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWER WE CAN VOTE FOR THEM', 'node': self.name}})
        self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination': rv['source'], 'voteGranted': True, 'term' : self.term})
        self.voted_for = rv['source']
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE VOTE GRANTED', 'node': self.name}})
        self.last_update = self.loop.time()
      else:
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE FOLLOWE WE VOTED OR THEY HAVE INVALID LOG', 'node': self.name}})
        self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination':rv['source'], 'voteGranted': False, 'term' : self.term})
      return

    else: # self.state == "candidate" or self.state == "leader":
      #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE CASE NOT FOLLOWER', 'node': self.name}})
      self.req.send_json({'type': 'requestVoteReply', 'source': self.name, 
          'destination':rv['source'], 'voteGranted': False, 'term' : self.term})
    return

  def handle_requestVoteReply(self, rvr):
    self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE REPLY', 'node': self.name}})
    if self.state == "candidate": #case candidate
      #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE REPLY CASE CANDIDATE', 'node': self.name}})
      if rvr['voteGranted'] == True:
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE REQUEST VOTE REPLY CASE CANDIDATE VOTE GRANTED', 'node': self.name}})
        if (rvr['source'] not in self.accepted):
          self.accepted.append(rvr['source'])
          if len(self.accepted) >= self.qorum:
            self.begin_term()
      else:
        if rvr['source'] not in self.refused:
          self.refused.append(rvr['source'])
    #otherwise ignore
    return

  def handle_appendEntries(self, msg):
    #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES', 'node': self.name}})
    if msg['term'] < self.term:
      #self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES THEIR TERM LESS THAN OURS', 'node': self.name}})
      self.req.send_json({'type': 'appendEntriesReply', 'source': self.name, 
          'destination': msg['source'], 'success': False, 'term' : self.term})
      return
    self.state = "follower"
    self.last_update = self.loop.time()
    self.leaderId = msg['source']
    while len(self.pending_sets) > 0:
      self.handle_set(self.pending_sets.pop(0))
    #Only do this fancy appendEntry logic if there's an entry in the message (o/w it must be a heartbeat / leader notification )
    #brilliant
    if msg['entries']:
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES NOT HEARTBEAT', 'node': self.name, 'term': self.term}})
      
      prevLogIndex = msg['prevLogIndex']
      prevLogTerm = msg['prevLogTerm']
      if len(self.log) < prevLogIndex: #case entry index too large; we are missing entries
        self.req.send_json({'type': 'appendEntriesReply', 'source': self.name, 
          'destination': msg['source'], 'success': False, 'term' : self.term})
      elif len(self.log) > 0 and self.log[prevLogIndex]['term'] != prevLogTerm: #case previous conflicting entries
        self.req.send_json({'type': 'appendEntriesReply', 'source': self.name, 
          'destination': msg['source'], 'success': False, 'term' : self.term})
      else: # case we can append entries 
        index = prevLogIndex + 1
        new_entries = msg['entries']
        for entry in new_entries: 
          if len(self.log) < index: #if index is greater than current length (not possible to have conflicting entries because none there)
            break
          elif self.log[index]['term'] != entry['term']: #remove any conflicting entry and any afterward
            while len(self.log) >= index:
              self.log.pop()
            break
          else:
            new_entries.pop(0)
            index += 1
        for entry in new_entries: #append new entries
          self.log.append({'key': entry['key'], 'value': entry['value'], 'term': entry['term']})
        last_log_index = len(self.log) - 1 
        last_log_term = self.log[last_log_index]['term']
        if (msg['leaderCommit'] > self.commit_index):
          self.commit_index = min( msg['leaderCommit'], len (self.log))
        self.req.send_json({'type': 'appendEntriesReply', 'source': self.name, 'destination': msg['source'], 'logLastIndex': last_log_index, 'logLastTerm': last_log_term, 'term':self.term, 'commitIndex': self.commit_index, 'success': True })


      '''
      if (len(self.log) < msg['prevLogIndex'] ):
        self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES LOG LENGTH SMALLER THAN MSG', 'node': self.name}})
        self.req.send_json({'type': 'appendEntriesReply', 'source': self.name, 'destination': msg['source'], 'success': False, 'term': self.term, 'prevLogIndex': self.last_log_index})
        return
      print len(self.log), "=?=", msg['prevLogIndex']
      if ( len(self.log) > 0 and self.log[msg['prevLogIndex']]['term'] != msg['prevLogTerm'] ):
        self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES MISMATCH TERMS', 'node': self.name, 'msgTerm': msg['prevLogTerm'], 'curterm': self.log[msg['prevLogIndex']]['term']}})
        self.log = self.log[:msg['prevLogIndex']]
        self.last_log_index = msg['prevLogIndex']
        self.last_log_term = msg['prevLogTerm']

        self.req.send_json({'type': 'appendEntriesReply', 'source': self.name, 'destination': msg['source'], 'success': False, 'term': self.term, 'prevLogIndex': self.last_log_index})
        return
      else:
        if ( len(self.log) > 0 and msg['leaderCommit'] > 0 and log[msg['leaderCommit']]['term'] != msg['term'] ):
          self.log = self.log[:self.commit_index]
          for entry in msg['entries']:
            self.log.append(entry)
            self.commit_index += 1
          self.last_log_index = len(log) - 1
          self.last_log_term = log[-1]['term']
          self.commit_index = len(log) -1
    self.apply_commits() 
          self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES LEADER COMMITTED BUT MISMATCH TERMS ', 'node': self.name}})
          self.req.send_json({'type': 'appendEntriesReply', 'success': True, 'source': self.name, 'destination': msg['source'], 
            'key': self.log[self.last_log_index]['key'], 'term': self.term }) 
      for entry in msg['entries']:
        self.log.append(entry)
      self.last_log_index = len(self.log) - 1
      self.last_log_term = self.log[-1]['term']
      self.commit_index = len(self.log) - 1
      self.apply_commits()
      self.req.send_json({'type': 'appendEntriesReply', 'source': self.name, 'destination': msg['source'], 'prevLogIndex': self.last_log_index, 'prevLogTerm': self.last_log_term, 
        'key': self.log[self.last_log_index]['key'], 'term':self.term, 'commitIndex': self.commit_index, 'success': True })
      self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES!', 'node': self.name, 'key': self.log[self.last_log_index]['key'], 'value': self.log[self.last_log_index]['value']}})
    ''' 
    return
  

  def handle_appendEntriesReply(self, msg):
    self.req.send_json({'type': 'log', 'debug': {'event': 'HANDLE APPEND ENTRIES REPLY ', 'node': self.name}})
    if self.state == "leader":
      if msg['success']:
        self.match_index[msg['source']] = msg['logLastIndex']
        self.next_index[msg['source']] = msg['logLastIndex'] + 1
  # if we know we have replicated an entry on a majority of the nodes, then we can safely set response
  #if num_matches >= qorum:
        # self.req.send_json({'type': "setResponse", 'id': msg['id'], 'value': msg['value']})
  # We should add the message ID to each of these messages (or some means of keeping track of set request msg ids), since we aren't sending the setResponse until we commit
      else: #failure
        self.next_index[msg['source']] += -1
        '''
        self.next_index[msg['source']] = msg['commitIndex'] + 1
        if self.appendVotes.has_key(msg['key']): #make sure we are voting on this key
        # V is this supposed to be the source name or dest name? I'm assuming source
        # Yes, this is the leader recording which followers have responded, which is the source of the Reply message.
          if msg['source'] not in self.appendVotes[msg['key']]: #dont allow repeat voting
            self.appendVotes[msg['key']].append(msg['source'])
            if len(self.appendVotes[msg['key']]) == self.qorum: #if qorum of followers have responded
              #self.log[self.term][msg['key']] = msg['value'] # comit value to log
              # ^ I think this line will look more like this:
              self.log.append({'term': self.term,'key': msg['key'], 'value': msg['value']})
              self.last_log_index = len(self.log) - 1
              self.last_log_term = self.term
              for peer in self.next_index.keys():
                next_index = self.next_index[peer]
                if next_index < self.last_log_index:
                  self.req.send_json({'type': 'appendEntries', 'prevLogIndex': self.next_index[msg['source']], 'prevTerm': self.last_log_term, 'leaderCommit': self.commit_index, 'source': self.name, 'destination': peer, 'entries': self.log[self.next_index[aer['source']]], 'leaderId': self.leaderId, 'term': self.term})
                  #send commit messages
                  #should we bother removing from logqueue and appendvotes here?
        else: #not sucess
          print 'hi', msg['success']  
          #force follower to copy log
    else:
      print "Warning, " + self.name + "recieved appendEntriesReply while not leader"
        '''
      return

  def housekeeping(self): #handles election BS
    now = self.loop.time()
    #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING TOP LEVEL', 'node': self.name}})
    if self.state == "follower":
      if now - self.last_update > term_timeout: #case of no heartbeats
        self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE FOLLOWER TERM TIMEOUT', 'node': self.name}})
        self.call_election()
        self.loop.add_timeout(min(self.election_timeout, now + polling_timeout), self.housekeeping)
      else:
        self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE FOLLOWER NO TIMEOUT', 'node': self.name}})
        self.loop.add_timeout(self.last_update + term_timeout, self.housekeeping)
    elif self.state == "candidate":
      if now < self.election_timeout: #case within an election but haven't won nor timeout occurred
        if len(self.refused) < self.qorum: #still chance of winning; poll more votes
          #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE CANDIDATE REPOLL', 'node': self.name}})
          self.poll()
          self.loop.add_timeout(min(self.election_timeout, now + polling_timeout), self.housekeeping)
        else: #no chance of winning election
          #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE CANDIDATE REFUSED > QORUM', 'node': self.name}})
          self.loop.add_timeout(self.election_timeout, self.housekeeping)
      else: # election timeout has occurred
        #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE CANDIDATE ELECTION TIMEOUT', 'node': self.name}})
        self.call_election()
        self.loop.add_timeout(min(self.election_timeout,now + polling_timeout), self.housekeeping)
    else: #case leader
      #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE LEADER', 'node': self.name}})
      self.leader_update_commitIndex()
      self.broadcast_heartbeat()
      #self.req.send_json({'type': 'log', 'debug': {'event': 'HOUSEKEEPING CASE LEADER FINISHED BROADCAST', 'node': self.name}})
      self.loop.add_timeout(now + heartbeat_timeout, self.housekeeping)
    return
  
  def call_election(self):
    if len(self.peer_names) > 0: #no need to poll if only one leader
      self.req.send_json({'type': 'log', 'debug': {'event': 'CALL ELECTION', 'node': self.name}})
      self.term += 1
      self.state = "candidate"
      self.accepted = []
      self.refused = []
      self.election_timeout = self.loop.time() + random.uniform(min_election_timeout, max_election_timeout)
      self.accepted.append(self)
      self.poll()
    else:
      self.req.send_json({'type': 'log', 'debug': {'event': 'ONLY ONE NODE, THUS LEADER', 'node': self.name}})
      self.begin_term()
    return
 
  def poll(self):
    #self.req.send_json({'type': 'log', 'debug': {'event': 'POLL', 'node': self.name}})
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
    for peer in self.peer_names:
      self.next_index[peer] = len(self.log)
      self.match_index[peer] = -1
    self.match_index[self.name] = len(self.log) - 1
    self.leaderId = self.name
    while len(self.pending_sets) > 0:
      self.handle_set(self.pending_sets.pop(0))
    #send append entries RPC to all others
    return

  def leader_update_commitIndex(self):
    match = self.match_index.values()
    match.sort()
    median = len(match)/2
    if match[median] > self.commit_index and self.log[match[median]]['term'] == self.term: #match[2] is the 2nd largest value in match_index_values, i.e. the median. 
      self.commit_index = match[2]

  def broadcast_heartbeat(self):
    #self.req.send_json({'type': 'log', 'debug': {'event': 'BROADCAST HEARTBEAT', 'node': self.name, 'peers' : self.peer_names}})
    for peer in self.peer_names:
      peerNextIndex = self.next_index[peer]
      myNextIndex = len(self.log)
      if peerNextIndex == myNextIndex:
        self.req.send_json({'type': 'log', 'debug': {'event': 'BROADCASTING HEARTBEAT', 'node': self.name}})
        self.req.send_json({'type': 'appendEntries', 'source': self.name, 
          'destination': peer, 'term': self.term, 'prevLogIndex': 0, 
          'prevLogTerm': 0, 'entries': None, 'leaderCommit': self.commit_index}) #*** this needs to be changed to reflect actual AE RPCs
      else:
        self.req.send_json({'type': 'log', 'debug': {'event': 'BROADCASTING AE RPC', 'node': self.name}})
        self.req.send_json({'type': 'appendEntries', 'source': self.name, 
          'destination': peer, 'term': self.term, 'prevLogIndex': 0, 
          'prevLogTerm': 0, 'entries': self.log[peerNextIndex:myNextIndex], 'leaderCommit': self.commit_index})
    return

  def manage_pending_sets(self):
    if self.state == "follower":
      if self.leaderId:
        for ID in self.pending_sets.keys():
          self.req.send_json({'type': 'forwardedSet', 'destination': self.leaderId, 'setRequest':self.pending_sets[ID]})
    elif self.state == "leader":
      for ID in self.pending_sets.keys():
        set_request = self.pending_sets.pop(ID)['setReqest']
        self.log.append({'key': set_request['key'], 'value': set_request['value'], 'term': self.term })
        self.next_index[self.name] = len(self.log)
        self.last_log_index = len(log) - 1
        self.last_log_term = self.term
        self.pending_sets2[self.last_log_index] = set_request
    return


  def apply_commits(self): #commit each log entry until the next commit index
    while self.commit_index > self.last_applied:
      self.last_applied += 1
      entry = self.log[self.last_applied]
      key = entry['key']
      value = entry['value']
      self.store[key] = value 
      if self.state == 'leader':
        self.req.send_json({'type': 'setResponse', 'id': msg['id'], 'value': msg['value']})
    self.loop.add_timeout(self.loop.time() + commit_timeout, self.apply_commits)
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
  if args.peer_names:  # ''.split(',') = [''] not []
    args.peer_names = args.peer_names.split(',')
  else:
    args.peer_names = []
  Node(args.node_name, args.pub_endpoint, args.router_endpoint, args.spammer, args.peer_names).start()
