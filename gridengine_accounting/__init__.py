# Copyright 2013 David Irvine
#
# This file is part of gridengine-accounting
#
# gridengine-accounting is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# gridengine-accounting is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with gridengine-accounting.  If not, see <http://www.gnu.org/licenses/>.
import json

class AccountFile:
	def __init__(self, file_ob):
		self._file_ob=file_ob
	def __iter__(self):
		return self
	def next(self):
		line=self._file_ob.readline()
		if not line:
			raise StopIteration
		while (line.startswith("#")):
			line=self._file_ob.readline()
		return AccountEntry(line)


class AccountEntry:
	def __init__(self,line):
		lines=line.split(":")
		if len(lines)!=45:
			raise ValueError("Line not of correct format")
		self._queue_name=lines.pop(0)
		self._hostname=lines.pop(0)
		self._group=lines.pop(0)
		self._owner=lines.pop(0)
		self._job_name=lines.pop(0)
		self._job_number=int(lines.pop(0))
		self._account=lines.pop(0)
		self._priority=int(lines.pop(0))
		self._submission_time=int(lines.pop(0))
		self._start_time=int(lines.pop(0))
		self._end_time=int(lines.pop(0))
		self._failed=int(lines.pop(0))
		self._exit_status=int(lines.pop(0))
		self._ru_wallclock=int(lines.pop(0))
		self._ru_utime=float(lines.pop(0))
		self._ru_stime=float(lines.pop(0))
		self._ru_maxrss=float(lines.pop(0))
		self._ru_ixrss=float(lines.pop(0))
		self._ru_ismrss=float(lines.pop(0))
		self._ru_idrss=float(lines.pop(0))
		self._ru_isrss=float(lines.pop(0))
		self._ru_minflt=float(lines.pop(0))
		self._ru_majflt=float(lines.pop(0))
		self._ru_nswap=float(lines.pop(0))
		self._ru_inblock=float(lines.pop(0))
		self._ru_oublock=float(lines.pop(0))
		self._ru_msgsnd=float(lines.pop(0))
		self._ru_msgrcv=float(lines.pop(0))
		self._ru_nsignals=float(lines.pop(0))
		self._ru_nvcsw=float(lines.pop(0))
		self._ru_nivcsw=float(lines.pop(0))
		self._project=lines.pop(0)
		if self._project=="NONE":
			self._project=None
		self._department=lines.pop(0)
		if self._department=="NONE":
			self._department=None
		self._granted_pe=lines.pop(0)
		if self._granted_pe=="NONE":
			self._granted_pe=None
		self._slots=int(lines.pop(0))
		self._task_number=int(lines.pop(0))
		self._cpu=float(lines.pop(0))
		self._mem=float(lines.pop(0))
		self._io=float(lines.pop(0))
		self._catagory=lines.pop(0)
		if self._catagory=="NONE":
			self._catagory=None
		self._iow=float(lines.pop(0))
		try:
			self._pe_taskid=int(lines.pop(0))
		except ValueError:
			self._pe_taskid=None
		self._maxvmem=float(lines.pop(0))
		self._arid=int(lines.pop(0))
		self._ar_submission_time=int(lines.pop(0))

	@property
	def queue_name(self):
		'''Name of the cluster queue in which the job has run.'''
		return u"%s" % self._queue_name

	@property
	def qname(self):
		'''Name of the cluster queue in which the job has run.'''
		return self.queue_name

	@property
	def hostname(self):
		'''Name of the execution host.'''
		return u"%s" % self._hostname

	@property
	def host_name(self):
		'''Name of the execution host.'''
		return self.hostname

	@property
	def group(self):
		'''The effective group id of the job owner when executing the job.'''
		return u"%s" % self._group

	@property
	def owner(self):
		'''Owner of the Sun Grid Engine job.'''
		return u"%s" % self._owner

	@property
	def job_name(self):
		'''Job name.'''
		return u"%s" % self._job_name

	@property
	def job_number(self):
		'''Job identifier - job number'''
		return self._job_number

	@property
	def account(self):
		'''An account string as specified by the qsub(1) or qalter(1) -A option.'''
		return u"%s" % self._account

	@property
	def priority(self):
		'''Priority value assigned to the job corresponding to the priority parameter in the queue configuration (see queue_conf(5)).'''
		return self._priority

	@property
	def submission_time(self):
		'''Submission time (GMT unix time stamp).'''
		return self._submission_time

	@property
	def start_time(self):
		'''Start time (GMT unix time stamp).'''
		return self._start_time

	@property
	def end_time(self):
		'''End time (GMT unix time stamp).'''
		return self._end_time

	@property
	def failed(self):
		'''Indicates the problem which occurred in case a job could not be started on the execution host (e.g. because the owner of the job did not have a valid account on that machine). If Sun Grid Engine tries to start a job multiple times, this may lead to multiple  entries  in  the  accounting file corresponding to the same job ID.'''
		return u"%s" % self._failed

	@property
	def exit_status(self):
		'''Exit  status  of  the job script (or Sun Grid Engine specific status in case of certain error conditions).  The exit status  is  determined  by following  the  normal  shell  conventions.   If the command terminates normally the value of the command is its exit status.  However, in  the case  that  the  command exits abnormally, a value of 0200 (octal), 128 (decimal) is added to the value of the command  to  make  up  the  exit status.

For  example:  If a job dies through signal 9 (SIGKILL) then the exit status becomes 128 + 9 = 137.'''
		return self._exit_status

	@property
	def ru_wallclock(self):
		'''Difference between end_time and start_time (see above)'''
		return self._ru_wallclock

	@property
	def ru_utime(self):
		'''user time used'''
		return self._ru_utime

	@property
	def ru_stime(self):
		'''system time used'''
		return self._ru_stime

	@property
	def ru_maxrss(self):
		'''maximum resident set size'''
		return self._ru_maxrss

	@property
	def ru_ixrss(self):
		'''integral shared memory size'''
		return self._ru_ixrss

	@property
	def ru_ismrss(self):
		return self._ru_ismrss

	@property
	def ru_idrss(self):
		'''integral unshared data size'''
		return self._ru_idrss

	@property
	def ru_isrss(self):
		'''integral unshared stack size'''
		return self._ru_isrss

	@property
	def ru_minflt(self):
		'''page reclaims'''
		return self._ru_minflt

	@property
	def ru_majflt(self):
		'''page faults'''
		return self._ru_majflt

	@property
	def ru_nswap(self):
		'''swaps'''
		return self._ru_nswap

	@property
	def ru_inblock(self):
		'''block input operations'''
		return self._ru_inblock

	@property
	def ru_oublock(self):
		'''block output operations'''
		return self._ru_oublock

	@property
	def ru_msgsnd(self):
		'''messages sent'''
		return self._ru_msgsnd

	@property
	def ru_msgrcv(self):
		'''messages received'''
		return self._ru_msgrcv

	@property
	def ru_nsignals(self):
		'''signals received'''
		return self._ru_nsignals

	@property
	def ru_nvcsw(self):
		'''voluntary context switches'''
		return self._ru_nvcsw

	@property
	def ru_nivcsw(self):
		'''involuntary context switches'''
		return self._ru_nivcsw

	@property
	def project(self):
		'''The project which was assigned to the job.'''
		return u"%s" % self._project

	@property
	def department(self):
		'''The department which was assigned to the job.'''
		return u"%s" % self._department

	@property
	def granted_pe(self):
		'''The parallel environment which was selected for that job.'''
		return u"%s" % self._granted_pe

	@property
	def slots(self):
		'''The  number of slots which were dispatched to the job by the scheduler.'''
		return self._slots

	@property
	def task_number(self):
		'''Array job task index number.'''
		return self._task_number

	@property
	def cpu(self):
		'''The cpu time usage in seconds.'''
		return self._cpu

	@property
	def mem(self):
		'''The integral memory usage in Gbytes cpu seconds.'''
		return self._mem

	@property
	def catagory(self):
		'''A string specifying the job category.'''
		return u"%s" % self._catagory

	@property
	def iow(self):
		'''The io wait time in seconds.'''
		return self._iow

	@property
	def pe_taskid(self):
		'''If this identifier is set the task was part of a parallel job  and  was passed to Sun Grid Engine via the qrsh -inherit interface.'''
		return self._pe_taskid

	@property
	def maxvmem(self):
		'''The maximum vmem size in bytes.'''
		return self._maxvmem

	@property
	def arid(self):
		'''Advance reservation identifier. If the job used resources of an advance reservation then this field contains a positive integer identifier otherwise the value is "0".'''
		return self._arid

	@property
	def ar_submission_time(self):
		'''If  the  job  used  resources of an advance reservation then this field contains the submission time (GMT  unix  time  stamp)  of  the  advance reservation, otherwise the value is "0".'''
		return  self._ar_submission_time
	def to_json(self):
		d={}
		for k,v in self.__dict__.iteritems():
			k=k.lstrip("_")
			d[k]=v
		return json.dumps(d)
