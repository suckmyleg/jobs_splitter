class jobs_splitter:
	def __init__(self, threads):
		if not threads:
			threads = 1

		self.threads = threads
		self.jobs = {}

	def get_new_job_id(self):
		while True:
			id = str(rd(0, 999))

			try:
				p = self.jobs[id]
			except:
				return id

	def do_job(self, job, elements, job_id, id):
		for e in elements:
			r = job(e)
			if not r == None:
				self.jobs[job_id]["values"][id].append(r)
		self.jobs[job_id]["status"][id] = True

	def split_job(self, job, elements, n=False):

		if not n:
			n = self.threads

		lenel = len(elements)

		if lenel == 0:
			return []
 
		id = self.get_new_job_id()

		self.jobs[id] = {"values":[[] for a in range(n)], "status":[False for a in range(n)]}

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

		for i in elements_splitted:
			t = Th(target=self.do_job, args=(job, i, id, elements_splitted.index(i)))
			t.start()
			threads.append(t)

		while True:
			done = True
			for s in self.jobs[id]["status"]:
				if s == False:
					done = False
			if done:
				break

		result = []

		for m in self.jobs[id]["values"]:
			result += m
		del self.jobs[id]
		return result
