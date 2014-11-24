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


class AccountFile(object):
    def __init__(self, file_ob):
        self._file_ob = file_ob
        self._row_num = 0

    def __iter__(self):
        return self

    def next(self):
        while True:
            self._row_num += 1
            line = self._file_ob.readline()
            if not line:
                raise StopIteration
            if line.startswith("#"):
                continue

            fields = line.split(":")
            if len(fields) in [45, 46]:
                try:
                    int(fields[0])  # standard SGE row doesnt start with int
                except ValueError:
                    return AccountEntry(line)
            elif len(fields) == 47 and fields[1] == "acct":
                return AccountEntry(line)
            elif len(fields) == 39:
                return AccountEntry(line)
            else:
                if fields[1] == "acct":
                    print "ERROR: Invalid length of accounting row, this is probably a big deal"
                print "Unknown Row Type: %d at line %d" % (len(fields), self._row_num)


class UGEAccountFile(AccountFile):
    """
    Iterator that returns a new UGEAccountEntry object for every valid row in an
    Univa Grid Engine Accounting file.

    Example::

        >>> from gridengine_accounting import UGEAccountFile
        >>> f = open("ug82_accounting")
        >>> for ac in UGEAccountFile(f):
        ...     print ac.job_number
        [...]

    """
    def next(self):
        while True:
            self._row_num += 1
            line = self._file_ob.readline()
            if not line:
                raise StopIteration
            if line.startswith("#"):
                continue
            return UGEAccountEntry(line)


class UGEAccountEntry(object):
    """
    .. py:attribute:: qname

        Name of the cluster queue in which the job has run.

    .. py:attribute:: hostname

        Name of the execution host.

    .. py:attribute:: group

        The effective group id of the job owner when executing the job.

    .. py:attribute:: owner

        Owner of the Univa Grid Engine job.

    .. py:attribute:: job_name

        Job name.

    .. py:attribute:: job_number

        Job identifier - job number.

    .. py:attribute:: account

        An account string as specified by the qsub(1) or qalter(1) -A option.

    .. py:attribute:: priority

        Priority value assigned to the job corresponding to the priority parameter in the queue configuration
        (see queue_conf(5)).

    .. py:attribute:: submission_time

        Submission time (64bit GMT unix time stamp in milliseconds).

    .. py:attribute:: start_time

        Start time (64bit GMT unix time stamp in milliseconds).

    .. py:attribute:: end_time

        End time (64bit GMT unix time stamp in milliseconds).

    .. py:attribute:: failed

        Indicates  the  problem  which  occurred in case a job could not be started on the execution host (e.g. because
        the owner of the job did not have a valid account on that machine). If Univa Grid Engine tries to start a
        job multiple times, this may lead to multiple entries in the accounting file corresponding to the same job ID.

    .. py:attribute:: exit_status

        Exit status of the job script (or Univa Grid Engine specific status in case of certain error conditions).  The
        exit status is determined by following  the  normal  shell conventions.   If  the  command terminates normally
        the value of the command is its exit status.  However, in the case that the command exits abnormally, a value
        of 0200 (octal), 128 (decimal) is added to the value of the command to make up the exit status.

        For example: If a job dies through signal 9 (SIGKILL) then the exit status becomes 128 + 9 = 137.

    .. py:attribute:: ru_wallclock

        Difference between end_time and start_time (see above).

    .. py:attribute:: ru_utime

        User CPU time used.  This is the total amount of time spent executing in user mode.

    .. py:attribute:: ru_stime

        System CPU time used.  This is the total amount of time spent executing in kernel mode.

    .. py:attribute:: ru_maxrss

        Maximum resident set size.  This is the maximum resident set size used (in kilobytes).

    .. py:attribute:: ru_ixrss

        Integral shared memory size.  This field is currently unused on Linux.

    .. py:attribute:: ru_ismrss

        This field is currently unused on Linux.

    .. py:attribute:: ru_idrss

        Integral unshared data size.  This field is currently unused on Linux.

    .. py:attribute:: ru_isrss

        Integral unshared stack size.  This field is currently unused on Linux.

    .. py:attribute:: ru_minflt

        Page reclaims (soft page faults)  The number of page faults serviced without any I/O activity; here I/O
        activity is avoided by "reclaiming" a page frame from the list of pages awaiting reallocation.

    .. py:attribute:: ru_majflt

        Page faults (hard page faults) The number of page faults serviced that required I/O activity.

    .. py:attribute:: ru_nswap

        Swaps.  This field is currently unused on Linux.

    .. py:attribute:: ru_inblock

        Block input operations.  The number of times the file system had to perform input.

    .. py:attribute:: ru_oublock

        Block output operations.  The number of times the file system had to perform output.

    .. py:attribute:: ru_msgsnd

        IPC messages sent.  This field is currently unused on Linux.

    .. py:attribute:: ru_msgrcv

        IPC messages received.  This field is currently unused on Linux.

    .. py:attribute:: ru_nsignals

        Signals received.  This field is currently unused on Linux.

    .. py:attribute:: ru_nvcsw

        Voluntary context switches.  The number of times a context switch resulted due to a process voluntarily giving
        up the processor before its time slice was completed (usually to await availability of a resource).

    .. py:attribute:: ru_nivcsw

        The number of times a context switch resulted due to a higher priority process becoming runnable or because
        the current process exceeded its time slice.

    .. py:attribute:: project

        The project which was assigned to the job.

    .. py:attribute:: department

        The department which was assigned to the job.

    .. py:attribute:: granted_pe

        The parallel environment which was selected for that job.

    .. py:attribute:: slots

        The number of slots which were dispatched to the job by the scheduler.

    .. py:attribute:: task_number

        Array job task index number.

    .. py:attribute:: cpu

        The cpu time usage in seconds.

    .. py:attribute:: mem

        The integral memory usage in Gbytes cpu seconds.

    .. py:attribute:: io

        The  amount  of data transferred in Gbytes.  On Linux data transferred means all bytes read and written by the
        job through the read(), pread(), write() and pwrite() systems calls.

    .. py:attribute:: category

        A string specifying the job category.

    .. py:attribute:: iow

        The io wait time in seconds.

    .. py:attribute:: pe_taskid

        If this identifier is set the task was part of a parallel job and was passed to Univa Grid Engine via the
        qrsh -inherit interface.

    .. py:attribute:: maxvmem

        The maximum vmem size in bytes.

    .. py:attribute:: arid

        Advance reservation identifier. If the job used resources of an advance reservation then this field contains a
        positive integer identifier otherwise the value is "0" .

    .. py:attribute:: ar_submission_time

        If the job used resources of an advance reservation then this field contains the submission time (64bit GMT
        unix time stamp in milliseconds) of the advance  reservation, otherwise the value is "0" .

    .. py:attribute:: job_class

        If the job has been running in a job class, the name of the job class, otherwise "NONE" .

    .. py:attribute:: qdel_info

        If  the job (the array task) has been deleted via qdel, "<username>@<hostname>", else "NONE".  If qdel was
        called multiple times, every invocation is recorded in a comma separated list.

    .. py:attribute:: maxrss

        The maximum resident set size in bytes.

    .. py:attribute:: maxpss

        The maximum proportional set size in bytes.

    .. py:attribute:: submit_host

        The submit host name.

    .. py:attribute:: cwd

        The working directory the job ran in as specified with qsub / qalter switches -cwd and -wd.

    .. py:attribute:: submit_cmd

        The command line used for job submission.

    """
    def __init__(self, line):
        fields = line.split(":")
        if len(fields) != 52:
            raise ValueError("Line contains invalid number of fields")

        self.qname = fields.pop(0)
        self.hostname = fields.pop(0)
        self.group = fields.pop(0)
        self.owner = fields.pop(0)
        self.job_name = fields.pop(0)
        self.job_number = int(fields.pop(0))
        self.account = fields.pop(0)
        self.priority = int(fields.pop(0))
        self.submission_time = int(fields.pop(0))
        self.submission_time_milliseconds = self.submission_time
        self.submission_time = float(self.submission_time)/1000
        self.start_time = int(fields.pop(0))
        self.start_time_milliseconds = int(self.start_time)
        self.start_time = float(self.start_time)/1000
        self.end_time = int(fields.pop(0))
        self.end_time_milliseconds = int(self.end_time)
        self.end_time = float(self.end_time)/1000
        self.failed = int(fields.pop(0))
        self.exit_status = int(fields.pop(0))
        self.ru_wallclock = fields.pop(0)
        self.ru_wallclock_milliseconds = self.ru_wallclock
        self.ru_wallclock = float(self.ru_wallclock) / 1000

        self.ru_utime = float(fields.pop(0))
        self.ru_stime = float(fields.pop(0))
        self.ru_maxrss = float(fields.pop(0))
        self.ru_ixrss = float(fields.pop(0))
        self.ru_ismrss = float(fields.pop(0))
        self.ru_idrss = float(fields.pop(0))
        self.ru_isrss = float(fields.pop(0))
        self.ru_minflt = float(fields.pop(0))
        self.ru_majflt = float(fields.pop(0))
        self.ru_nswap = float(fields.pop(0))
        self.ru_inblock = float(fields.pop(0))
        self.ru_oublock = float(fields.pop(0))
        self.ru_msgsnd = float(fields.pop(0))
        self.ru_msgrcv = float(fields.pop(0))
        self.ru_nsignals = float(fields.pop(0))
        self.ru_nvcsw = float(fields.pop(0))
        self.ru_nivcsw = float(fields.pop(0))
        self.project = fields.pop(0)
        self.department = fields.pop(0)
        self.granted_pe = fields.pop(0)
        self.slots = int(fields.pop(0))
        self.task_number = int(fields.pop(0))
        self.cpu = float(fields.pop(0))
        self.mem = float(fields.pop(0))
        self.io = float(fields.pop(0))
        self.category = fields.pop(0)
        self.iow = float(fields.pop(0))
        self.pe_taskid = fields.pop(0)
        self.maxvmem = int(fields.pop(0))
        self.arid = int(fields.pop(0))
        self.ar_submission_time = int(fields.pop(0))
        self.ar_submission_time_milliseconds = self.ar_submission_time
        self.ar_submission_time = float(self.ar_submission_time) / 1000
        self.job_class = fields.pop(0)
        self.qdel_info = fields.pop(0)
        self.maxrss = int(fields.pop(0))
        self.maxpss = int(fields.pop(0))
        self.submit_host = fields.pop(0)
        self.cwd = fields.pop(0)
        self.submit_cmd = fields.pop(0)
        self.submit_cmd.replace("\255", ":")

    def to_dict(self):
        """
        Returns a dictionary of the accounting file entry.

        :return: Accounting entry as dictionary.
        :rtype: dict
        """
        return self.__dict__

class AccountEntry:
    def __init__(self, line):
        lines = line.split(":")

        if len(lines) not in [45, 46, 47, 39]:
            raise ValueError("Line not of correct format")
        if len(lines) == 47:  # Remove type and timestime fields.
            lines.pop(0)
            lines.pop(0)
        self._qname = lines.pop(0)
        self._hostname = lines.pop(0)
        self._group = lines.pop(0)
        self._owner = lines.pop(0)
        self._job_name = lines.pop(0)
        self._job_number = int(lines.pop(0))
        self._account = lines.pop(0)
        self._priority = int(lines.pop(0))
        self._submission_time = int(lines.pop(0))
        self._start_time = int(lines.pop(0))
        self._end_time = int(lines.pop(0))
        self._failed = int(lines.pop(0))

        if len(lines) == 39:
            self._exit_status = 0  # Not present in base for univa UD
        else:
            self._exit_status = int(lines.pop(0))

        self._ru_wallclock = int(lines.pop(0))
        self._ru_utime = float(lines.pop(0))
        self._ru_stime = float(lines.pop(0))
        self._ru_maxrss = float(lines.pop(0))
        self._ru_ixrss = float(lines.pop(0))
        self._ru_ismrss = float(lines.pop(0))
        self._ru_idrss = float(lines.pop(0))
        self._ru_isrss = float(lines.pop(0))
        self._ru_minflt = float(lines.pop(0))
        self._ru_majflt = float(lines.pop(0))
        self._ru_nswap = float(lines.pop(0))
        self._ru_inblock = float(lines.pop(0))
        self._ru_oublock = float(lines.pop(0))
        self._ru_msgsnd = float(lines.pop(0))
        self._ru_msgrcv = float(lines.pop(0))
        self._ru_nsignals = float(lines.pop(0))
        self._ru_nvcsw = float(lines.pop(0))
        self._ru_nivcsw = float(lines.pop(0))
        self._project = lines.pop(0)
        if self._project == "NONE":
            self._project = None
        self._department = lines.pop(0)
        if self._department == "NONE":
            self._department = None
        self._granted_pe = lines.pop(0)
        if self._granted_pe == "NONE":
            self._granted_pe = None
        self._slots = int(lines.pop(0))
        self._task_number = int(lines.pop(0))
        self._cpu = float(lines.pop(0))
        self._mem = float(lines.pop(0))
        self._io = float(lines.pop(0))

        self._catagory = lines.pop(0)
        if self._catagory == "NONE":
            self._catagory = None
        if len(lines) in [45, 46]:
            self._iow = float(lines.pop(0))
        else:
            self._iow = 0.0  # Not present in Univa UD

        self._pe_taskid = None  # Not present in Univa UD
        if len(lines) in [45, 46]:
            try:
                self._pe_taskid = int(lines.pop(0))
            except ValueError:
                pass

        self._maxvmem = 0.0
        if len(lines) in [45, 46]:
            self._maxvmem = (lines.pop(0))

        self._arid = 0  # Not present in Univa UD
        if len(lines) in [45, 46]:
            self._arid = int(lines.pop(0))

        self._ar_submission_time = float(lines.pop(0))

    @property
    def queue_name(self):
        """Name of the cluster queue in which the job has run."""
        return u"%s" % self.qname

    @property
    def qname(self):
        """Name of the cluster queue in which the job has run."""
        return self._qname

    @property
    def hostname(self):
        """Name of the execution host."""
        return u"%s" % self._hostname

    @property
    def host_name(self):
        """Name of the execution host."""
        return self.hostname

    @property
    def group(self):
        """The effective group id of the job owner when executing the job."""
        return u"%s" % self._group

    @property
    def owner(self):
        """Owner of the Sun Grid Engine job."""
        return u"%s" % self._owner

    @property
    def job_name(self):
        """Job name."""
        return u"%s" % self._job_name

    @property
    def job_number(self):
        """Job identifier - job number"""
        return self._job_number

    @property
    def account(self):
        """An account string as specified by the qsub(1) or qalter(1) -A option."""
        return u"%s" % self._account

    @property
    def priority(self):
        """Priority value assigned to the job corresponding to the priority parameter in the queue configuration
        (see queue_conf(5))."""
        return self._priority

    @property
    def submission_time(self):
        """Submission time (GMT unix time stamp)."""
        return self._submission_time

    @property
    def start_time(self):
        """Start time (GMT unix time stamp)."""
        return self._start_time

    @property
    def end_time(self):
        """End time (GMT unix time stamp)."""
        return self._end_time

    @property
    def failed(self):
        """Indicates the problem which occurred in case a job could not be started on the execution host (e.g. because
        the owner of the job did not have a valid account on that machine). If Sun Grid Engine tries to start a job
        multiple times, this may lead to multiple  entries  in  the  accounting file corresponding to the same job
        ID."""
        return u"%s" % self._failed

    @property
    def exit_status(self):
        """Exit  status  of  the job script (or Sun Grid Engine specific status in case of certain error conditions).
        The exit status  is  determined  by following  the  normal  shell  conventions.   If the command terminates
        normally the value of the command is its exit status.  However, in  the case  that  the  command exits
        abnormally, a value of 0200 (octal), 128 (decimal) is added to the value of the command  to  make  up  the
        exit status.

        For  example:  If a job dies through signal 9 (SIGKILL) then the exit status becomes 128 + 9 = 137."""
        return self._exit_status

    @property
    def ru_wallclock(self):
        """Difference between end_time and start_time (see above)"""
        return self._ru_wallclock

    @property
    def ru_utime(self):
        """user time used"""
        return self._ru_utime

    @property
    def ru_stime(self):
        """system time used"""
        return self._ru_stime

    @property
    def ru_maxrss(self):
        """maximum resident set size"""
        return self._ru_maxrss

    @property
    def ru_ixrss(self):
        """integral shared memory size"""
        return self._ru_ixrss

    @property
    def ru_ismrss(self):
        return self._ru_ismrss

    @property
    def ru_idrss(self):
        """integral unshared data size"""
        return self._ru_idrss

    @property
    def ru_isrss(self):
        """integral unshared stack size"""
        return self._ru_isrss

    @property
    def ru_minflt(self):
        """page reclaims"""
        return self._ru_minflt

    @property
    def ru_majflt(self):
        """page faults"""
        return self._ru_majflt

    @property
    def ru_nswap(self):
        """swaps"""
        return self._ru_nswap

    @property
    def ru_inblock(self):
        """block input operations"""
        return self._ru_inblock

    @property
    def ru_oublock(self):
        """block output operations"""
        return self._ru_oublock

    @property
    def ru_msgsnd(self):
        """messages sent"""
        return self._ru_msgsnd

    @property
    def ru_msgrcv(self):
        """messages received"""
        return self._ru_msgrcv

    @property
    def ru_nsignals(self):
        """signals received"""
        return self._ru_nsignals

    @property
    def ru_nvcsw(self):
        """voluntary context switches"""
        return self._ru_nvcsw

    @property
    def ru_nivcsw(self):
        """involuntary context switches"""
        return self._ru_nivcsw

    @property
    def project(self):
        """The project which was assigned to the job."""
        return u"%s" % self._project

    @property
    def department(self):
        """The department which was assigned to the job."""
        return u"%s" % self._department

    @property
    def granted_pe(self):
        """The parallel environment which was selected for that job."""
        return u"%s" % self._granted_pe

    @property
    def slots(self):
        """The  number of slots which were dispatched to the job by the scheduler."""
        return self._slots

    @property
    def task_number(self):
        """Array job task index number."""
        return self._task_number

    @property
    def cpu(self):
        """The cpu time usage in seconds."""
        return self._cpu

    @property
    def mem(self):
        """The integral memory usage in Gbytes cpu seconds."""
        return self._mem

    @property
    def catagory(self):
        """A string specifying the job category."""
        return u"%s" % self._catagory

    @property
    def iow(self):
        """The io wait time in seconds."""
        return self._iow

    @property
    def pe_taskid(self):
        """If this identifier is set the task was part of a parallel job  and  was passed to Sun Grid Engine via the
        qrsh -inherit interface."""
        return self._pe_taskid

    @property
    def maxvmem(self):
        """The maximum vmem size in bytes."""
        return self._maxvmem

    @property
    def arid(self):
        """Advance reservation identifier. If the job used resources of an advance reservation then this field contains
        a positive integer identifier otherwise the value is "0"."""
        return self._arid

    @property
    def ar_submission_time(self):
        """If  the  job  used  resources of an advance reservation then this field contains the submission time (GMT
        unix  time  stamp)  of  the  advance reservation, otherwise the value is "0"."""
        return self._ar_submission_time

    def to_json(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        d = {}
        for k, v in self.__dict__.iteritems():
            k = k.lstrip("_")
            d[k] = v
        return d