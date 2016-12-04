from aws_tools import *
from scheduler import *
import pprint
scheduler_info_path = "../temp/scheduler_info.json"
status = load_scheduler_status(scheduler_info_path)
display_scheduler_info(status)
results_dir = status['option']['results_dir']
job_done_dir = status['option']['job_done_dir']

# scheduler_info_log="../temp/scheduler_info_log.json"
#status_log = read_json_file(scheduler_info_log)
# polling(status)

# Get IP List
IPs = []
for key, sq in status['sQueues'].iteritems():
    for item in sq:
        IPs.append(item['PublicIpAddress'])

# Find a job
JobId = 'exp_695553'
key_pair_path = '~/SECURITY_FILE/hyunwoo_key.pem'
for sip in IPs:
    prefix = "ssh -q -oStrictHostKeyChecking=no -i " + key_pair_path + ' ubuntu@' + sip
    try:
        #command = prefix+' ls | grep '+JobId
        command = prefix + ' ls '
        output = run_shell_check_output(command, debug=True)
        if len(output) > 0:
            print output
    except:
        continue
#        print 'Err:'+command
"""
# Clean snodes    
key_pair_path='~/SECURITY_FILE/hyunwoo_key.pem'
for IP in IPs:
    cleanup_node(IP, key_pair_path)
"""
"""
y_scheduler_info(status):qa
input_dir = status['option']['input_dir']
key_pair_path = status['option']['key_pair_path']
servers = status['servers']
print status['sQueues']
node =pop_node_from_squeues_by_InstanceId(status, 'i-f838427e')
print status['sQueues']
append_node_to_squeues(status, 'Terminated', node)
print status['sQueues']
#transfer_security_file_to_servers(input_dir, key_pair_path, servers)
print status['sQueues']
terminate_idle_servers(status)
print "terminate_idle_servers(status)"
status['sQueues']['Running'][0]['JobIds']=[]

print "terminate_idle_servers(status)"
terminate_idle_servers(status)
print status['sQueues']

display_jQueues(status)
display_sQueues(status)

JobId = status['jobs'][0]['JobId']
dst_ip = servers[0]['PublicIpAddress'] 
#polling(status)
#run run_scheduler ../temp/option.json
node = servers[0]
local_run_script=status['option']['local_run_script']
binpath=status['option']['binpath']
key_pair_path=status['option']['key_pair_path']
myip=status['MasterIp']
results_dir=status['option']['results_dir']
raw_result_fname='p_value.txt'
job_done_dir=status['option']['job_done_dir']
run_job_on_a_remote_server(node,local_run_script,JobId,binpath,\
                key_pair_path,myip,results_dir,raw_result_fname,job_done_dir)
"""
