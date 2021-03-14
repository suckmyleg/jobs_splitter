from random import randint as rd
from threading import Thread as Th
from time import sleep

class jobs_splitter:
	def __init__(self, threads=False, delete_job=True, debug=False):
		if not threads:
			threads = 1

		self.threads = threads
		self.jobs = {}
		self.delete_job = delete_job
		self.debug = debug

	def get_new_job_id(self):
		while True:
			id = str(rd(0, 999))

			try:
				p = self.jobs[id]
			except:
				return id

	def info(self, job_id):
		while True:
			try:
				print(job_id)
				for t in self.jobs[job_id]["workers_status"]:
					print(t)
				print("")
			except:
				return True
			#sleep(0.001)

	def wait_until(self, job_id, status):
		while True:
			try:
				#print(job_id, self.jobs)
				now_status = self.jobs[job_id]["status"]
			except:
				return True

			if now_status == status:
				break
			sleep(0.01)

	def do_job(self, job, elements, job_id, worker_id):
		self.jobs[job_id]["workers_status"][worker_id] = 0
		self.wait_until(job_id, 0)
		for e in elements:
			try:
				r = job(e)
			except Exception as error:
				r = None
				print("Jobs_splitter: \n--Error: {}\n--Worker_id: {}\n--Job_id: {}\n--Job: {}\n--Element: {}".format(error, worker_id, job_id, job, e))
			if not r == None:
				self.jobs[job_id]["values"][worker_id].append(r)
		self.jobs[job_id]["workers_status"][worker_id] = 1

	def reload_status(self, job_id):
		while True:
			status = self.get_status(job_id)
			self.jobs[job_id]["status"] = status
			if status == 1:
				break

	def split_job(self, job, elements, n=False):

		if not n:
			n = self.threads

		lenel = len(elements)

		if lenel == 0:
			return []
 
		id = self.get_new_job_id()

		self.jobs[id] = {"values":[[] for a in range(n)], "workers_status":[2 for a in range(n)], "status":2}

		threads = []

		l = int(lenel/n)
		if lenel%n > 0:
			l += 1

		elements_splitted = [[] for a in range(n)]
		i = 0
		e = 0
		for element in elements:
			i += 1
			elements_splitted[e].append(element)
			if i == l:
				i = 0
				e += 1


		t = Th(target=self.reload_status, args=(id, ))
		t.start()
		threads.append(t)

		if self.debug:
			t = Th(target=self.info, args=(id, ))
			t.start()
			threads.append(t)

		worker_id = 0
		#print("Starting job: {}".format(id))
		for i in elements_splitted:
			if self.debug:
				pass
				#print("Worker: {}".format(worker_id))
			t = Th(target=self.do_job, args=(job, i, id, worker_id))
			t.start()
			threads.append(t)
			worker_id += 1


		self.wait_until(id, 1)

		result = []

		for m in self.jobs[id]["values"]:
			result += m
		if self.delete_job:
			del self.jobs[id]
		return result

	def get_status(self, id):
		statuses = self.jobs[id]["workers_status"]

		if 0 in statuses:
			return 0
		else:
			if 2 in statuses:
				return 2
			else:
				if 1 in statuses:
					return 1


