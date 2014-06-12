group members:

Nathan Bartley: bartleyn@uchicago.edu
Aaron Schaer: aaschaer@uchicago.edu
Joshua Stevens-Stein: jstevensstein@gmail.com

WODUNIT
	Josh:
		-leadership functionality Pseudocode 
		-leadership functionality code
		-2nd and thid implementations of set (the version with the two generals issue, the call election version)
		-conception and implementation of phantom log entries
		-

	
	Nathan
		-Applying entries from the log to the state machine, pseudocode and implementation
		-Working with Aaron on the second implementation of the get requests
		-Working on forwarding client requests to the leader, before we decided to opt for availability
		-Designing the structure of the log
		-Testing scripts
		

How to run a node:
	python broker.py -e "python raft_node.py" -s <script name>
