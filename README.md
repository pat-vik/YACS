# Yet Another Centralized Scheduler (YACS)

• Implemented a centralized scheduling framework to simulate scheduling algorithms in a distributed setting.

• The simulated cluster consists of 1 Master process and 3 Worker processes. The Master listens for job requests and assigns them to Workers based on a scheduling algorithm.

• Implemented scheduling algorithms like random, round-robin and least-loaded to schedule jobs.


Scheduling	Algorithms:

1. Random: The Master chooses a machine at random. It then checks if the machine has free slots available. If yes, it launches the task on the machine. Else, it chooses another	machine at random. This	process	continues	until	a	free slot is found.

2. Round-Robin: The	machines are ordered based on worker_id of the Worker running on the machine. The	Master picks a machine in	round-robin	fashion. If the machine does not have	a free slot, the Master moves	on to	the	next worker_id in the	ordering. This process continues until a free	slot is	found.

3. Least-Loaded: The Master looks	at the state of	all	the	machines and checks	which	machine has	most number	of free	slots. It	then launches	the	task on	that machine. If none of the machines have free	slots	available, the Master waits	for	1	second and repeats the process. This process continues until a free	slot is found.
