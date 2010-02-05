#! /usr/bin/python
from distutils.text_file import TextFile as T 
import subprocess as SUB
from optparse import OptionParser
import sys,time
import signal,socket
import glob,os,shutil,random
import cPickle

#-------------------------------------------------------------------------
# DEFAULTS
#-------------------------------------------------------------------------
processPriority_DEFAULT=50
polling_interval_DEFAULT=60   # supervisor: polling seconds
running_time_DEFAULT=0        # running time: unlimited
#
reqMinProcessors_DEFAULT=1
reqFreeProcessor_DEFAULT=15 # processor is considered free if occupancy is below x%
#
processDB_DEFAULT=os.path.join(os.path.dirname(sys.argv[0]),"processDB")
processDB_QUEUE="queue"
processDB_CUR="cur"
processDB_DONE="done"
#
processDB_suffix_info=".info"
_DEBUG=50
#-------------------------------------------------------------------------
def option_parser():
    usage = "usage: %prog [options] [commands]"
    parser = OptionParser(usage=usage)
    # parser configuration
    parser.add_option("-s", "--submit",   action="store_true", dest="action_submit",  help="action: run")
    parser.add_option("-q", "--queue",    action="store_true", dest="action_queue",   help="action: queue traverse")
    parser.add_option("-H", "--halt",     action="store_true", dest="action_halt",    help="action: halt")
    parser.add_option("-i", "--info",     action="store_true", dest="action_info",    help="action: info")
    #
    parser.add_option("-n", "--processName", dest="processName", help="process name",default="")
    parser.add_option("-p", "--priority",  type="int", dest="priority", help="process priority. default: %d" % processPriority_DEFAULT,default=processPriority_DEFAULT)
    parser.add_option("-P", "--polling-interval",  type="int", dest="polling_interval", help="supervisor: polling interval. default: %d" % polling_interval_DEFAULT,default=polling_interval_DEFAULT)
    parser.add_option("-R", "--run-at-most",  type="int", dest="running_time", help="supervisor: run at most N seconds. default: %d (0: unlimited)" % running_time_DEFAULT,default=running_time_DEFAULT)
    #
    parser.add_option("-S", "--systemName",    dest="reqSystemName",   help="requirement: system name",default=None)
    parser.add_option("-C", "--min-processors", dest="reqMinProcessors", type="int", help="requirement: minimum free processors",default=reqMinProcessors_DEFAULT)
    parser.add_option("-M", "--min-memory",     dest="reqMinMemory",     type="int", help="requirement: minimum free memory (Mb)",default=None)
    #
    parser.add_option("--processDB", dest="processDB", help="process database: default=%s" % processDB_DEFAULT,default=processDB_DEFAULT)
    parser.add_option("--processDBqueue", dest="processDB_QUEUE", help="process database: queue: default=%s" % processDB_QUEUE,default=processDB_QUEUE)
    parser.add_option("--processDBcur",   dest="processDB_CUR", help="process database: current: default=%s" % processDB_CUR,default=processDB_CUR)
    parser.add_option("--processDBdone",  dest="processDB_DONE", help="process database: done: default=%s" % processDB_DONE,default=processDB_DONE)
    # parser usage
    (options, args) = parser.parse_args()
    # defaults
    if not( options.action_submit or options.action_queue or \
            options.action_halt or options.action_info ):
        # no actions, and arguments? submit to queue
        if len(args)>0:
            options.action_submit=True
        # no actions, nor arguments? process queue
        else:
            options.action_queue=True
    ##    parser.print_help(); sys.exit(0)
    #
    return (options, args)
#-------------------------------------------------------------------------
def time_format(time_struct):
    return time.strftime( "%Y-%m-%d %H:%M:%S", time_struct )
def write_file(fH,msg,endline=True,flush=True,timestamp=True):
    if timestamp:
        fH.write( "%s " % time_format(time.localtime()) )
    fH.write(msg)
    if endline: fH.write("\n")
    if flush: fH.flush()
#-------------------------------------------------------------------------
def get_sysName():
    return socket.gethostname()
def get_sysLogin():
    return os.getlogin()
def resources_check(options,args):
    # returns: True:  requirements met
    #          False: requirements not met
    check_result=True
    # check: machine name
    if not(options.reqSystemName is None):
        if not( options.reqSystemName.lower()==get_sysName().lower() ):
            check_result=False
    # check: free memory
    if not(options.reqMinMemory is None or options.reqMinMemory<=0):
        free_mem=SUB.Popen('free -m| grep "\-/+"', stdout=SUB.PIPE, shell=True).communicate()[0]
        free_mem=int(free_mem.split()[-1])
        if not( options.reqMinMemory <= free_mem ):
            check_result=False
    # check: free processors
    cores_num=int(SUB.Popen('grep "core id" /proc/cpuinfo | wc -l', stdout=SUB.PIPE, shell=True).communicate()[0])
    if cores_num==0:
        cores_num=int(SUB.Popen('grep "physical id" /proc/cpuinfo | sort -u | wc -l', stdout=SUB.PIPE, shell=True).communicate()[0])
    assert cores_num > 0, "# ERROR: cpu number not identified: cpus='%s'" % cores_num
    # gets the heaviest processes
    cpus_occupancy=map( float,  # max(0,float(x)-reqFreeProcessor_DEFAULT)
        SUB.Popen('ps -eo pcpu | sort -rg | head -n %d' % (3*cores_num), stdout=SUB.PIPE, shell=True).communicate()[0].split() )
    if _DEBUG>=100:
        print "# DEBUG: cpu occupancy:",cpus_occupancy
    cpus_occupancy=reduce( lambda x,y: float(x)+float(y), cpus_occupancy )
    free_cpus=int( cores_num - (cpus_occupancy-reqFreeProcessor_DEFAULT)/100 )
    if not( options.reqMinProcessors <= free_cpus ):
        check_result=False
    # return resources check response
    if _DEBUG>=50:
        print "# DEBUG: requirements check:", check_result
    return check_result
#-------------------------------------------------------------------------
def initialize_dirs(options,args):
    for dir in ( options.processDB_QUEUE, options.processDB_CUR, options.processDB_DONE ):
        dir_full=os.path.join(options.processDB,dir)
        if not os.path.isdir(dir_full):
            os.makedirs( dir_full )
class process:
    # process initialization and signaling
    def __init__(self,
            cmd,
            shell=True, close_fds=True,
            stdin=None, stdout=None, stderr=SUB.STDOUT,
            processDB=None, processName="",
            sys_name=get_sysName(),
            process_submitter=get_sysLogin(),
            process_requester=None
            ):
        #
        self.processDB=processDB
        self.processDB_CUR=processDB_CUR
        self.processDB_DONE=processDB_DONE
        self.processName=processName
        #
        self.sys_name=sys_name
        self.process_submitter=process_submitter
        self.process_requester=process_requester
        #
        self._initialize(
            cmd, shell=shell, close_fds=close_fds,
            stdin=stdin, stdout=stdout, stderr=stderr,
            )
    #
    def _initialize(self,
            cmd, shell, close_fds,
            stdin, stdout, stderr,
            ):
        #processFile_last=hashlib.sha1("%s%s%.3f" % ( sys_name,process_submitter,time.time()) ).hexdigest()
        processFile_pre=",".join( (
            self.sys_name,
            self.process_submitter,self.process_requester,
            self.processName,
            str(os.getpid()),
            str(time.time()),
            str(random.random()),
            ) )
        # replace path separator string
        processFile_pre=processFile_pre.replace(os.path.sep,"_")
        self.processFileInfo=os.path.join(self.processDB,self.processDB_CUR,processFile_pre+processDB_suffix_info)
        if stdout is None:
            self.processFileOut= os.path.join(self.processDB,self.processDB_CUR,processFile_pre+".out")
            self.p_outH=open(self.processFileOut,"a")
        else:
            self.p_outH=stdout
        self.p_infoH=open(self.processFileInfo,"a")
        #
        self.p=SUB.Popen(
            cmd,
            shell=shell, close_fds=close_fds,
            stdin=stdin, stdout=self.p_outH, stderr=stderr)
        # write process info
        self.write_info( "# process info: '%s'" % self.processFileInfo )
        self.write_info( "# process output: '%s'" % self.processFileOut )
        self.write_info( "# process_name: %s, sys_name: %s, process_submitter: %s, process_requester: %s, pid: %d" % (
            self.processName,self.sys_name,self.process_submitter,self.process_requester,self.get_pid() )
            )
        self.write_info( "# cmd: %s" % cmd )
        #
        if _DEBUG>=10:
            print "# DEBUG: process info file: '%s'" % (self.processFileInfo)
    def get_pid(self):
        return self.p.pid
    #
    def poll(self):
        return self.p.poll()
    def signalSend(self,signal=signal.SIGKILL):
        self.p.send_signal(signal)
    #
    def poll_info(self):
        self.write_info( "# poll %d: %s" % (self.get_pid(),self.poll()) )
    def write_info(self,msg):
        write_file( self.p_infoH, msg )
    def closeHandles(self):
        self.p_outH.close()
        self.p_infoH.close()
    def archive_DONE(self):
        shutil.move(self.processFileInfo,self.processFileInfo.replace(self.processDB_CUR,self.processDB_DONE))
        shutil.move(self.processFileOut,self.processFileOut.replace(self.processDB_CUR,self.processDB_DONE))
#-------------------------------------------------------------------------
def run(options,args):
    executable=args[0]
    params=args[1:]
    process_requester=os.getlogin()
    result=_run(
            executable, params,
            options.processDB,
            process_requester=process_requester,
            polling_interval=options.polling_interval,
            running_time=options.running_time,
            processName=options.processName,
        )
    return result
def _run(executable,params=[],processDB="process_logdir",
        process_requester=None,
        polling_interval=polling_interval_DEFAULT,
        running_time=running_time_DEFAULT,
        processName="",
        ):
    # command to be executed
    cmd=str(executable)+" "+' '.join(params)
    # process: spawn
    p = process(cmd,processName=processName,process_requester=process_requester,processDB=processDB)
    # process: controller
    time_start=time.time()
    while p.poll() is None:
        p.poll_info()
        if running_time>0 and (running_time<=(time.time()-time_start)):
            p.signalSend(SUB.signal.SIGKILL)
            p.write_info( "# halting: overtime: %.1fs elapsed, %ds requested" % ( time.time()-time_start, running_time ) )
            # wait for the process to die, then check again
            time.sleep(1)
            continue
        time.sleep(polling_interval)
    # process: finished
    p.write_info( "# elapsed: %.1fs, return_code: %d" % (time.time()-time_start,p.poll()) )
    p.closeHandles()
    # move process files to DONE directory
    p.archive_DONE()
    return True
#-------------------------------------------------------------------------
def queue_submit(
        options,args,
        processDB=None,
        processDB_QUEUE=processDB_QUEUE,
        qFilename=None,
        ):
    # defaults
    if processDB is None:
        processDB=options.processDB
    if qFilename is None:
        qFilename="p%02d-t%s-P%d-%s" % ( options.priority,str(time.time()),os.getpid(),str(random.random()) )
    #
    if options.action_submit or len(args)>0:
        qFilenameFull=os.path.join(options.processDB,processDB_QUEUE,qFilename)
        qH=open(qFilenameFull,"wb")
        #write_file(qH,"(%s,%s)" % (options,args),timestamp=False)
        cPickle.dump( (options,args), qH )
        qH.close()
#-------------------------------------------------------------------------
def queue_traverse(options,args,process_flag_in_file=","):
    #
    queue_list=glob.glob(os.path.join(options.processDB,options.processDB_QUEUE,"*"))
    queue_list.sort()
    if _DEBUG>=100:
        print "# DEBUG: queue list:",queue_list
    #
    for qFilenameFull in queue_list:
        # is file being processed elsewhere?
        if process_flag_in_file in qFilenameFull:
            continue
        #
        if _DEBUG>=10: print "# DEBUG: queue process:",qFilenameFull
        # acquire lock, else skip
        qFilenameFull_new=qFilenameFull+process_flag_in_file+str(random.random())
        try:
            shutil.move(qFilenameFull,qFilenameFull_new)
        except IOError:
            continue
        # read queued file
        qH=open(qFilenameFull_new,"rb")
        #(options,args)=eval( qH.read() )
        (options_job,args_job)=cPickle.load(qH)
        qH.close()
        # are resources requirement met?
        if _DEBUG>=100:
            print "# DEBUG: resource check"
        if not( resources_check(options,args) ):
            # place job back in the queue, continue to process next job
            shutil.move(qFilenameFull_new,qFilenameFull)
            continue
        # execute
        if _DEBUG>=100:
            print "# DEBUG: execution"
        execution_result=run(options_job,args_job)
        # execution finished?
        if execution_result==True:
            os.unlink(qFilenameFull_new)
#-------------------------------------------------------------------------
# MAIN
#-------------------------------------------------------------------------
if __name__=="__main__":
    (options, args)=option_parser()
    if _DEBUG>=10:
        print "# DEBUG: (%s,%s)" % (options,args)
    # initialize DB tree
    initialize_dirs(options,args)
    resources_check(options,args) # TODO: REMOVE
    # submit to queue
    if options.action_submit:
        if _DEBUG>=50: print "# DEBUG: queue: submit"
        queue_submit(options,args)
    # process queue
    if options.action_queue:
        if _DEBUG>=50: print "# DEBUG: queue: traverse"
        queue_traverse(options,args)
    #
    if options.action_info:
        print "UNINPLEMENTED..."
    #
    if options.action_halt:
        print "UNINPLEMENTED..."

"""
#-------------------------------------------------------------------------
def status(processDB="process_logdir",processDB_suffix_info=processDB_suffix_info):
    filename_list=glob.glob(os.path.join(processDB,"*"+processDB_suffix_info))
    filename_list.sort()
    for filename in filename_list:
        record=os.path.basename(filename).replace(processDB_suffix_info,"")
        record=record.split(",")
        if len(record)<4: continue
        #
        fH=open(filename,"r")
        fH_content=fH.read()
        fH.close()
        #
        if "finished" in fH_content: record_finished=True
        else: record_finished=False
        #
        print time.strftime( "%Y-%m-%d %H:%M:%S", time.localtime(float(record[4])) ),
        if record_finished:
            print "finished",
        #print cmd,
        print record
"""
