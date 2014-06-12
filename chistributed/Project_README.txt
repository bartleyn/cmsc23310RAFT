group members:

Nathan Bartley: bartleyn@uchicago.edu
Aaron Schaer: aaschaer@uchicago.edu
Joshua Stevens-Stein: jstevensstein@gmail.com

WODUNIT
	Josh:
		-Leadership functionality Pseudocode 
		-Leadership functionality code
		-Second and third implementations of set (the version with the two generals issue, the call election version)
		-Conception and implementation of phantom log entries

	Aaron:
		-Log replication pseudocode
		-First implementation of set
		-Co-worked on second implementation of get
		-Testing scripts
	
	Nathan
		-Applying entries from the log to the state machine, pseudocode and implementation
		-Working with Aaron on the second implementation of the get requests
		-Working on forwarding client requests to the leader, before we decided to opt for availability
		-Designing the structure of the log
		-Testing scripts
		

How to run a node:
	python broker.py -e "python raft_node.py" -s <script name>
