import json
import os
import sys
from aws_tools import *
from urllib2 import urlopen
from collections import deque
from copy import deepcopy
import time
import pprint
import paramiko

Mypp = pprint.PrettyPrinter()


def get_available_space(dst_ip, key_pair_path):
    print("I am inside get_available_space")
    print('"%s" : "%s"' % ("destination ip is", dst_ip))
    k = paramiko.RSAKey.from_private_key_file(key_pair_path)
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect(hostname=dst_ip, username="ubuntu", pkey=k)
    stdin, stdout, stderr = c.exec_command("df -h / | awk 'NR==2{print $4}'")
    available_space = stdout.read()
    c.close()
    moment = time.strftime("%Y-%b-%d__%H_%M_%S", time.localtime())
    space_file = open('available_space_' + moment + '.txt', 'w')
    space_file.write(available_space)
    space_file.close()
    return available_space


def noneNstr(s):
    if s is None:
        return 'None'
    return s

# Status {Running, Stopped, Terminated
# In JSON output
# node['State']['Name'] -> 'running', 'stopped', 'terminated'

# All properties for a node.
# node['InstanceId'] : 'i-0d165891' # Example
# 0 (pending), 16 (running), 32 (shutting-down), 48 (terminated), 64 (stopping), and 80 (stopped)
# pending | running | shutting-down | terminated | stopping | stopped

# node['State']    : 'running'|'stopped'|'terminating'|'terminated'
# node['JobId']    : 'exp_1' | '1'
# node['JobState'] : 'Ready' (copied) | 'running' | 'completed' | 'failed' : Not necessary
# node['PublicIpAddress'] : '52.207.239.237' | None

# Job List
# Job['JobId'] : 'exp_1' | '1'
# Job['State'] : 'start' | 'ready' (copied) | 'running' | 'completed' | 'failed'
# Job['InstanceId'] : InstanceId of assigned node/server


def init_jobs(input_dir):
    jids = get_JobIds(input_dir)
    jobs = []
    for j in jids:
        job = {}
        job['JobId'] = j  # Job Id is the same as its subdirectory name
        job['State'] = None
        job['InstanceId'] = None
        jobs.append(job)
    return jobs


def init_servers(InstanceIds, key_pair_path, cleanup=False):
    servers = []
    for InstanceId in InstanceIds:
        output = run_shell_check_output_json("aws ec2 describe-instances "
                                             + "--instance-ids " + InstanceId)
        inst = output['Reservations'][0]['Instances'][0]
        IP = inst['PublicIpAddress']
        assert isIP(IP)
        if cleanup == True:
            cleanup_node(IP, key_pair_path)

        node = {}
        node['InstanceId'] = inst['InstanceId']
        node['State'] = inst['State']['Name']
        node['PublicIpAddress'] = IP
        node['JobIds'] = []
        node['nCPUs'] = get_num_cpus(IP, key_pair_path)
        servers.append(node)
    return servers


def init_scheduler(jobs, servers, option):
    # Job queues
    # Job Start Queue : Basic information of jobs
    # Job Ready Queue : Jobs are copied to slave node (copying one by ones)
    # Job Running Queue : Jobs are copied to slave node (copying one by ones)
    # Job Completed Queue : Jobs are copied to slave node (copying one by ones)

    # Server queues
    # Server Start Queue : Just spawned and before 2/2 check passed.
    # Server Running Queue : 2/2 check passed. (only running & 2/2 check)
    # Server Stopped Queue : stopped by accident or
    #                        no job will be assigned to this server.
    #                        (include shutting-down or terminating)
    # Server Terminated Queue : "terminated"

    status = {}
    status['jobs'] = jobs
    status['servers'] = servers
    status['option'] = option

    myip = urlopen('http://ip.42.pl/raw').read()
    status['MasterIp'] = myip
    status['MasterInstanceId'] = get_my_InstanceId()
    jQueues = {}
    jStartQueue = deque()
    put_items_in_deque(jStartQueue, jobs)
    jQueues['Start'] = jStartQueue  # jStartQueue
    jQueues['Ready'] = deque()  # jReadyQueue
    jQueues['Running'] = deque()  # jRunningQueue
    jQueues['Completed'] = deque()  # jCompletedQueue

    sQueues = {}
    sQueues['Start'] = deque()

    sRunningQueue = deque()
    put_items_in_deque(sRunningQueue, servers)
    sQueues['Running'] = sRunningQueue  # running servers
    sQueues['Stopped'] = deque()
    sQueues['Terminated'] = deque()
    status['jQueues'] = jQueues
    status['sQueues'] = sQueues
    status['Change'] = True
    return status


def copy_jobs_to_servers_wrapper(input_dir, key_pair_path, jobs, servers):
    assert check_enough_servers(jobs, servers)
    for i in range(len(jobs)):
        JobId = jobs[i]['JobId']
        servers['JobId'] = jobId
#        servers['JobState'] = 'Ready'
        jobs[i]['InstanceId'] = node['InstanceId']


def check_enough_servers(jobs, servers):
    njobs = 0
    for job in jobs:
        if job['InstanceId'] == None:
            njobs = njobs + 1
    nservers = 0
    for server in servers:
        if server['State'] == 'running' and server['JobId'] == None:
            nservers = nservers + 1
    return njobs == nservers


def copy_jobs_to_servers(input_dir, key_pair_path, JobIds, dst_ips, local_run_scrip=None, debug_level=0):
    print "before assert"
    assert len(JobIds) == len(dst_ips)
    for i in range(len(JobIds)):
        print "after assert"
        print(dst_ips[i])
        # check if slaves have enough space
        if get_available_space(dst_ips[i], key_pair_path) < '1G':
            # kill all the instances and terminate them
            return
        else:
            copy_job_to_server(input_dir, key_pair_path, JobIds[
                               i], dst_ips[i], debug_level=debug_level)


def transfer_security_file_to_servers(input_dir, key_pair_path, servers):
    for node in servers:
        transfer_security_file_to_server(
            input_dir, key_pair_path, node['PublicIpAddress'])


def transfer_security_file_to_server(input_dir, key_pair_path, dst_ip):
    prefix = "scp -q -oStrictHostKeyChecking=no -i " + key_pair_path
    run_shell(prefix + " " + key_pair_path + " ubuntu@" + dst_ip + ":~/")


def copy_job_to_server(input_dir, key_pair_path, JobId, dst_ip, local_run_script=None, debug_level=0):
    if debug_level > 0:
        print("Copy job {0} to Server at {1}".format(JobId, dst_ip))
    prefix = "scp -q -oStrictHostKeyChecking=no -i " + key_pair_path
    joinpath = os.path.join
    if debug_level > 1:
        debug = True
    else:
        debug = False
    run_shell(prefix + " -r " + joinpath(input_dir, JobId) +
              " ubuntu@" + dst_ip + ":~/", debug)
    run_shell(prefix + " " + joinpath(input_dir, "shared/*") + " ubuntu@" + dst_ip
              + ":~/" + JobId, debug)
    if local_run_script is not None:
        run_shell(prefix + " " + joinpath(local_run_script) + " ubuntu@" + dst_ip
                  + ":~/" + JobId, debug)


def save_scheduler_status(fname, status, msg=None):
    status['last_update'] = now().strftime("%Y%m%d_%H%M%S")
    if msg is not None:
        status['message'] = msg
    status_list = deepcopy(status)
    # convert deque to list to save them in a json file.
    convert_deque_to_list(status_list['sQueues'])
    convert_deque_to_list(status_list['jQueues'])
    write_json_file(fname, status_list)
    return status_list


def convert_deque_to_list(dict_of_deques):
    for key in dict_of_deques:
        dict_of_deques[key] = list(dict_of_deques[key])


def convert_list_to_deque(dict_of_lists):
    for key in dict_of_lists:
        dict_of_lists[key] = deque(dict_of_lists[key])


def save_scheduler_status_wrapper(fname, status, msg=None, logpath=None,
                                  changeonly=True):
    # fname is for status
    status_in_list = save_scheduler_status(fname, status, msg=msg)
    if logpath is not None:
        if os.path.exists(logpath):
            log = read_json_file(logpath)
        else:
            log = []
        if status['change'] or not changeonly:
            log.append(status_in_list)
            status['change'] = False
        write_json_file(logpath, log)


def load_scheduler_status(fname):
    status = read_json_file(fname)
    convert_list_to_deque(status['jQueues'])
    convert_list_to_deque(status['sQueues'])
    return status


def transfer_file(job, node, key_pair_path, input_dir, local_run_script=None, debug_level=0):
    copy_job_to_server(input_dir, key_pair_path, job['JobId'], node[
                       'PublicIpAddress'], local_run_script, debug_level)

    # Update Job and Server status
    job['State'] = 'Ready'
    job['InstanceId'] = node['InstanceId']
    node['JobIds'].append(job['JobId'])
    return (job, node)


def transfer_files_wrapper(status, key_pair_path, input_dir):
    jobs = status['jobs']
    nodes = status['servers']
    assert len(jobs) == len(nodes)
    for i in range(len(jobs)):
        transfer_file(jobs[i], nodes[i], key_pair_path, input_dir)


def transfer_files4script(slave_ip_path, key_pair_path, input_dir, local_run_script):
    """ This is for transfer_files_to_slaves script. It is obsolete."""
    jobnames = get_dirnames(input_dir)
    IPs = get_strlist_from_text(slave_ip_path)
    if "shared" in jobnames:
        jobnames.remove("shared")
    assert len(jobnames) == len(IPs)
    for i in range(len(IPs)):
        print("Server [{0}]: {1} for {2}".format(i, IPs[i], jobnames[i]))
        prefix = "scp -q -oStrictHostKeyChecking=no -i " + key_pair_path
        run_shell(prefix + " -r " + input_dir + "/" +
                  jobnames[i] + " ubuntu@" + IPs[i] + ":~/")
        run_shell(prefix + " " + key_pair_path + " ubuntu@" + IPs[i] + ":~/")
        run_shell(prefix + " -r " + input_dir +
                  "/shared" + " ubuntu@" + IPs[i] + ":~/")
        # No alive ssh connection.
        run_shell(prefix + " " + local_run_script +
                  " ubuntu@" + IPs[i] + ":~/" + jobnames[i])
    return (jobnames, IPs)


def write_job_ip_tab(jobnames, IPs, job_ip_tab_path):
    fout = open(job_ip_tab_path, 'w')
    for i in range(len(jobnames)):
        fout.write(jobnames[i] + "," + IPs[i] + "\n")


def run_job_on_a_remote_server(node, local_run_script, JobId, binpath, key_pair_path, myip, results_dir, raw_result_fname, job_done_dir):
    IP = node['PublicIpAddress']
    # The location of key_pair_file in remote servers
    key_pair_file = os.path.basename(key_pair_path)
    command = "ssh -q -oStrictHostKeyChecking=no -i " + key_pair_path +\
        " ubuntu@" + IP + " 'bash -s' < " + local_run_script + " " + JobId + " " + binpath + " " +\
        key_pair_file + " " + myip + " " + results_dir + \
        " " + raw_result_fname + " " + job_done_dir + " &"
    run_shell(command)


def run_jobs_on_remote_servers(status, local_run_script, binpath, key_pair_path, myip, results_dir):
    jobs = status['jobs']
    nodes = status['servers']
    for job in jobs:
        node = get_node_by_InstanceId(nodes, job['InstanceId'])
        run_job_on_a_remote_server(
            node, local_run_script, binpath, key_pair_path, myip, results_dir)
#        node['JobState'] = 'running'
        job['State'] = 'running'


def get_node_by_InstanceId_wrapper(status, InstanceId, key=None):
    if None:
        return get_node_by_InstanceId(status['servers'], InstanceId)
    else:
        return get_node_by_InstanceId(status['sQueues'][key], InstanceId)


def get_node_by_InstanceId(nodes, InstanceId):
    for node in nodes:
        if node['InstanceId'] == InstanceId:
            return node
    return None


def get_node_by_JobId(status, JobId):
    job = get_job_by_JobId(status['jobs'], JobId)
    InstanceId = job['InstanceId']
    node = get_node_by_InstanceId(status['servers'], InstanceId)
    return node


def get_job_by_JobId(jobs, JobId):
    for job in jobs:
        if job['JobId'] == JobId:
            return job
    return None


def terminate_idle_servers(status):
    nodes = status['sQueues']['Running']
    InstanceIds = []
    for node in nodes:
        if len(node['JobIds']) == 0:
            node['State'] = 'terminating'  # Put it terminated queue
            InstanceIds.append(node['InstanceId'])

    for InstanceId in InstanceIds:
        terminate_servers_by_InstanceIds(node['InstanceId'], notermination=status[
                                         'option']['notermination'])
        nnode = pop_node_from_squeues_by_InstanceId(status, InstanceId)
        print "terminate: " + nnode['InstanceId']
        print nnode
        append_node_to_squeues(status, 'Terminated', nnode)


def append_node_to_squeues(status, key, node):
    status['sQueues'][key].append(node)


def pop_node_from_squeues_by_InstanceId(status, InstanceId):
    for key in status['sQueues']:
        someq = status['sQueues'][key]
        node = _pop_node_from_squeues_by_InstanceId(someq, InstanceId)
        if node is not None:
            return node
    return None


def _pop_node_from_squeues_by_InstanceId(someq, InstanceId):
    thenode = None
    for node in someq:
        if node['InstanceId'] == InstanceId:
            thenode = node

    if thenode is not None:
        someq.remove(thenode)
        return thenode


def wait_and_terminate_servers(status, results_dir, wait_time=10):
    jobs = status['jobs']
    nodes = status['servers']
    while True:
        time.sleep(wait_time)
        fnames = get_filenames(results_dir)
        job_ids = get_job_ids_from_filenames(fnames)

        for jobid in job_ids:
            job = get_job_by_JobId(jobs, jobid)
            InstanceId = job['InstanceId']
            State = get_server_state_by_InstanceId(InstanceId)
            node = get_node_by_InstanceId(nodes, InstanceId)
            if State == 'stopped' and node['State'] == 'running':
                job['State'] = 'completed'
                node['State'] = 'stopped'
                terminate_servers_by_InstanceIds(InstanceId)

            if State == 'terminated' or State == None:
                node['State'] = 'terminated'

        if check_all_jobs_completed(status) and\
           check_all_servers_terminated(status):
            break

    return 0


def update_jobs_with_completed_jobs(status, results_dir):
    jobs = status['jobs']
    nodes = status['servers']
    fnames = get_filenames(results_dir)
    job_ids = get_job_ids_from_filenames(fnames)

    for jid in job_ids:
        job = get_job_by_JobId(jobs, jid)
        job['State'] = 'completed'
        node = get_node_by_InstanceId(nodes, job['InstanceId'])


def check_running_jobs_completed(status, results_dir, job_done_dir):
    fnames = get_filenames(results_dir)
    job_ids_with_res = get_job_ids_from_filenames(fnames)
    fnames_copied = get_filenames(results_dir)

    # Check whether results are properly copied.
    job_ids_copied = get_job_ids_from_filenames(fnames_copied)
    jQueue = status['jQueues']
    tmp_queue = deque()
    for job in jQueue['Running']:
        jid = job['JobId']
        if jid in job_ids_with_res and jid in job_ids_copied:
            node = mark_job_complete(status, job)
            remove_completed_job_from_snode(status, job, node)
        else:
            tmp_queue.append(job)
    jQueue['Running'] = tmp_queue

#>>> Working on here
# snode means slave node


def remove_completed_job_from_snode(status, job, node):
    key_pair_path = status['option']['key_pair_path']

    # 0 nothing, 1 n jobs in queues, 2 details
    if int(status['option']['debug_level']) == 0:
        debug = False
    else:
        debug = True

    prefix = "ssh -q -oStrictHostKeyChecking=no -i " + key_pair_path + ' ubuntu@'\
        + node['PublicIpAddress']
    command = prefix + " rm -rf " + job['JobId']
    run_shell_check_output(command, debug=debug)


def mark_job_complete(status, job):
    # Job should be moved to jQueue['Completed']
    status['jQueues']['Completed'].append(job)
    # Update job info
    job['State'] = 'Completed'
    # Update Server info

    node = get_node_by_InstanceId_wrapper(
        status, job['InstanceId'], key='Running')
    if node is None:
        print "Warning: No node was found by " + job['InstanceId']
    else:
        job['InstanceId'] = None
        node['JobIds'].remove(job['JobId'])
    return node


def check_all_jobs_completed(status):
    jQueues = status['jQueues']
    return len(jQueues['Start']) == 0 and len(jQueues['Ready']) == 0 and\
        len(jQueues['Running']) == 0 and len(
            jQueues['Completed']) == len(status['jobs'])


def check_all_servers_terminated(status):
    sQueues = status['sQueues']
    return len(sQueues['Start']) == 0 and len(sQueues['Running']) == 0 and\
        len(sQueues['Stopped']) == 0 and len(sQueues['Terminated']) == len(status['servers'])\
        and (_check_all_servers_state_terminated(status) or status['option']['notermination'])


def _check_all_servers_state_terminated(status):
    for node in status['sQueues']['Terminated']:
        if node['State'] != 'terminated':  # _state_ is lowercase
            return False
    return True

# Working on here.


def get_total_num_cpus(status, state='running'):
    ncpus = 0
    for server in status['servers']:
        if server['State'] == state:
            ncpus = ncpus + server['nCPUs']
    return ncpus


def get_total_num_idle_cpus(status, offset=0):
    # offset can be used to spare cpus for OS.
    ncpus = 0
    for server in status['sQueues']['Running']:
        ncpus = ncpus + server['nCPUs'] - len(server['JobIds']) - offset
    return ncpus


def put_items_in_deque(q, item_list):
    for item in item_list:
        q.append(item)


def display_jQueues(status, debug_level=2):
    if debug_level == 0:
        return None
    if debug_level == 2:
        output_str = '\n=== jQueues ===\n'
        for key in status['jQueues']:
            jq = status['jQueues'][key]
            output_str = output_str + key + ':' + ','.join([item['JobId'] + '(' +
                                                            noneNstr(item['State']) + ')' for item in jq]) + '\n'
    if debug_level == 1:
        output_str = '(jQueues) ' +\
            ','.join([key + ':' + str(len(status['jQueues'][key]))
                      for key in status['jQueues']])
    print output_str


def display_sQueues(status, debug_level=2):
    if debug_level == 0:
        return None
    output_str = '\n=== sQueues ===\n'
    for key in status['sQueues']:
        sq = status['sQueues'][key]
        output_str = output_str + key + ':' + ','.join([item['InstanceId'] + '(' +
                                                        noneNstr(item['State']) + ',' + str(len(item['JobIds'])) + ')' for
                                                        item in sq]) + '\n'

    if debug_level == 1:
        output_str = '(sQueues) ' +\
            ','.join([key + ':' + str(len(status['sQueues'][key]))
                      for key in status['sQueues']])
    print output_str


def display_scheduler_info(status, debug_level=0):
    if debug_level == 2:
        try:
            print "\n==== Jobs ==="
            Mypp.pprint(status['jobs'])
            print "\n==== Servers ==="
            Mypp.pprint(status['servers'])
            display_jQueues(status)
            display_sQueues(status)
        except:
            return None
    if debug_level == 1:
        try:
            display_jQueues(status, debug_level)
            display_sQueues(status, debug_level)
        except:
            return None


def update_server_states_in_terminatedQ(status, debug=False):
    for node in status['sQueues']['Terminated']:
        if node['State'] == 'terminating':
            state = get_server_state_by_InstanceId(
                node['InstanceId'], debug=debug)
            if state == 'terminated':
                node['State'] = 'terminated'
                if debug:
                    print 'Update_server_states_in_terminatedQ'
                    Mypp.pprint(node)


def polling(status):
    # Once all servers are up, the scheduler will not spawn any servers later.
    results_dir = status['option']['results_dir']
    job_done_dir = status['option']['job_done_dir']
    scheduler_info_path = status['option']['scheduler_info_path']
    scheduler_log_path = status['option']['scheduler_log_path']
    log_change_only = status['option']['log_change_only']
    job_done_dir = status['option']['job_done_dir']
    # 0 nothing, 1 n jobs in queues, 2 details
    debug_level = int(status['option']['debug_level'])
    local_run_script = status['option']['local_run_script']

    while True:
        nCPUs = get_total_num_cpus(status)
        if nCPUs == 0:
            print 'No CPU is available.'
            break

        nCPUs_idle = get_total_num_idle_cpus(status)
        nIncompleteJobs = len(status['jQueues']['Start'])
        MasterIp = status['MasterIp']
        key_pair_path = status['option']['key_pair_path']
        input_dir = status['option']['input_dir']
        local_run_script = status['option']['local_run_script']
        binpath = status['option']['binpath']
        results_dir = status['option']['results_dir']
        raw_result_fname = status['option']['raw_result_fname']

        # Check any job is completed
        # Not real time we can move this to inner loop
        display_scheduler_info(status, debug_level)
        myprint("check_running_jobs_completed", debug_level)
        # Check running jobs by result_dir, job_done_dir
        # Move to Completed job
        check_running_jobs_completed(status, results_dir, job_done_dir)
        display_scheduler_info(status, debug_level)
        save_scheduler_status_wrapper(
            scheduler_info_path, status, "check_running_jobs_completed", scheduler_log_path, log_change_only)
        status = load_scheduler_status(scheduler_info_path)

        # Terminate servers
        if len(status['jQueues']['Start']) <= 0:  # The number of jobs not running
            terminate_idle_servers(status)
            myprint("terminate_idle_servers", debug_level)
            display_scheduler_info(status, debug_level)

        #  Double check servers' states are terminated.
        update_server_states_in_terminatedQ(status, debug=False)

        # Terminate scheduler.
        if check_all_jobs_completed(status) and check_all_servers_terminated(status):
            break

        # Transfer files to slave nodes and run them until nCPUs_ilde is zero.
        while True:
            nCPUs_idle = get_total_num_idle_cpus(status)
            print('nCPUs_idle')
            print(nCPUs_idle)
            nIncompleteJobs = len(status['jQueues']['Start'])
            if nCPUs_idle <= 0 or nIncompleteJobs <= 0:
                break
            # Fine a server with an idle cpu.
            myprint('Pop job and server', debug_level)
            display_scheduler_info(status, debug_level)
            job = status['jQueues']['Start'].popleft()
            node = status['sQueues']['Running'].popleft()

            # Copy files to a slave node.
            transfer_file(job, node, key_pair_path, input_dir,
                          local_run_script, debug_level)
            time.sleep(1)
            myprint('transfer_file ' + job['JobId'] +
                    ' ' + node['InstanceId'], debug_level)
            display_scheduler_info(status, debug_level)
            save_scheduler_status_wrapper(
                scheduler_info_path, status, "transfer_files_wrapper", scheduler_log_path, log_change_only)

            # Run the job on the remote server
            run_job_on_a_remote_server(node, local_run_script, job['JobId'], binpath,
                                       key_pair_path, MasterIp, results_dir, raw_result_fname, job_done_dir)
            myprint('run_job_on_a_remote_server ' +
                    job['JobId'] + ' ' + node['InstanceId'], debug_level)
            display_scheduler_info(status, debug_level)

            if node['nCPUs'] == len(node['JobIds']):
                status['sQueues']['Running'].append(node)
            else:
                status['sQueues']['Running'].appendleft(node)
            status['jQueues']['Running'].append(job)

            save_scheduler_status_wrapper(
                scheduler_info_path, status, "run_jobs_on_remote_servers", scheduler_log_path, log_change_only)
            status = load_scheduler_status(scheduler_info_path)


def myprint(msg, debug_level):
    if debug_level > 0:
        print msg
