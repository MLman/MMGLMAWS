from __future__ import print_function
import json
import os
import sys
import subprocess
import time
from datetime import datetime
import pdb
import re


now = datetime.now


def read_json_file(fname):
    fin = open(fname, 'r')
    parsed_json = json.load(fin)
    fin.close()
    return parsed_json


def write_json_file(fname, data):
    fout = open(fname, 'w')
    json.dump(data, fout)
    fout.close()


def get_ReservationId(fname):
    start_log = read_json_file(fname)
    ReservationId = start_log['ReservationId']
    assert len(ReservationId) > 0 and (type(ReservationId) == str
                                       or type(ReservationId) == unicode), 'ReservationId ' + ReservationId
    return ReservationId


def get_reservation_by_ReservationId(fname, ReservationId):
    servers = read_json_file(fname)
    for r in servers['Reservations']:
        if r['ReservationId'] == ReservationId:
            break
    return r


def get_InstanceId_by_ReservationId(fname, ReservationId):
    reservation = get_reservation_by_ReservationId(fname, ReservationId)
    InstanceIds = []
    for node in reservation['Instances']:
        InstanceIds.append(node['InstanceId'])
    return InstanceIds


def get_PublicIpAddress_by_ReservationId(fname, ReservationId):
    reservation = get_reservation_by_ReservationId(fname, ReservationId)
    PublicIpAddresses = []
    for node in reservation['Instances']:
        PublicIpAddresses.append(node['PublicIpAddress'])
    return PublicIpAddresses


def get_PubIpAddr_N_InsId_by_ReservationId(fname, ReservationId):
    reservation = get_reservation_by_ReservationId(fname, ReservationId)
    IP_N_Ids = []
    for node in reservation['Instances']:
        IP_N_Ids.append((node['InstanceId'], node['PublicIpAddress']))
    return IP_N_Ids


def get_attributes_by_Reservation(fname, ReservationId, attrnames):
    reservation = get_reservation_by_ReservationId(fname, ReservationId)
    info = []
    for node in reservation['Instances']:
        attrs = {}
        for a in attrnames:
            attrs[a] = node[a]
        info.append(attrs)
    return info


def get_dirnames(thedir, chkfile=None):
    if len(thedir) == 0:
        thedir = os.getcwd()
    elif type(thedir) is str or type(thedir) is unicode:
        thedir = thedir
    else:
        thedir = thedir[0]

    if chkfile is None:
        counteddirs = [name for name in os.listdir(
            thedir) if os.path.isdir(os.path.join(thedir, name))]
    else:
        counteddirs = [name for name in os.listdir(thedir) if os.path.isdir(os.path.join(thedir, name))
                       and os.path.isfile(os.path.join(thedir, name, chkfile))]
    return counteddirs


def get_strlist_from_text(fname):
    f = open(fname)
    text = f.read()
    f.close()
    return filter(len, text.split('\n'))


def get_JobIds(input_dir, ignore_subdirs=["shared"]):
    jobnames = get_dirnames(input_dir)
    for ig in ignore_subdirs:
        if ig in jobnames:
            jobnames.remove(ig)
    return jobnames


def spawn_servers(image_id="ami-b0f13081", security_group_ids="",
                  key_pair_name="", server_type="m4.4xlarge", num_servers=""):
    command_prefix = "aws ec2 run-instances --image-id " + image_id + \
        (" --security-group-ids " + security_group_ids if len(security_group_ids)
         > 0 else "") + " --count "
    command_suffix = " --instance-type " + \
        server_type + " --key-name " + key_pair_name

    command = command_prefix + num_servers + command_suffix
    command_for_one = command_prefix + "1" + command_suffix

    output = run_shell_check_output_json(command)
    ReservationId = output['ReservationId']

    # Get InstanceIds of One reservations
    InstanceIds = []
    for instance in output['Instances']:
        InstanceIds.append(instance['InstanceId'])

    # wait until all isntanced are ready.
    time.sleep(10)
    print('Wait until servers are ready.')
    debug = False
    while True:
        IsReady, InstanceIds = check_servers_are_ready_by_InstanceIds \
            (InstanceIds, command_for_one, debug)

        print(".", end='')  # Sort of status bar
        sys.stdout.flush()
        if my_all(IsReady):
            break
        else:
            debug = False
        time.sleep(10)

    print("\nAll servers are ready now.")
    return InstanceIds
    # return ReservationId


def get_filename_from_path(path):
    fname = os.path.basename(path)
    return os.path.splitext(fname)[0]


def run_shell(command, debug=True):
    if debug:
        print(command)
    subprocess.call(command, shell=True)


def run_shell_check_output(command, debug=True):
    if debug:
        print(command)
    output = subprocess.check_output(command, shell=True)
    return output


def run_shell_check_output_json(command, debug=True):
    output = run_shell_check_output(command, debug)
    parsed_output = json.loads(output)
    return parsed_output


def write_option_json(fname, keyname, keyvalue):
    option = {}
    if os.path.exists(fname):
        option = read_json_file(fname)
    option[keyname] = keyvalue
    write_json_file(fname, option)


def check_servers_are_ready_by_InstanceIds(InstanceIds, spawncom, debug=True):
    # Check mulitple instances by one query.
    # InstanceIds is a list of InstanceIds.
    # Also, if some servers fail to get ready (terminated), spawn new servers.
    # >>> True/False, InstanceId = check_servers_are_ready_by_InstanceIds('InstanceId')
    # >>> [True/False,...], InstanceIds = check_servers_are_ready_by_InstanceIds(['InstanceId',...])
    # spawncom is aws command to spawn a server.

    if type(InstanceIds) is str or type(InstanceIds) is unicode:
        InstanceIds = [InstanceIds]
    IDs = " ".join(InstanceIds)
    IsReady = []

    for i in range(len(InstanceIds)):
        InstanceId = InstanceIds[i]
        try:
            state = get_server_state_by_InstanceId(InstanceId)
            if state == 'terminated':
                output = run_shell_check_output_json(spawncom)
                # Get InstanceId of new server.
                InstanceIds[i] = output['Instances'][0]['InstanceId']
                print("New server [{0}] is spawned since [{1}] is terminated."
                      .format(InstanceIds[i], InstanceId))
                IsReady.append(False)
                continue

            if state == 'stopped':
                output = start_instance_by_InstanceId(InstanceId)
                print("Server [{0}] is restarted since it was terminated. "
                      .format(InstanceId))
                IsReady.append(False)
                continue

            status = run_shell_check_output_json \
                ("aws ec2 describe-instance-status --instance-ids " + InstanceId, debug=debug)
            instance = status['InstanceStatuses'][0]
            if instance['InstanceStatus']['Status'] != 'ok' \
                    or instance['SystemStatus']['Status'] != 'ok':
                IsReady.append(False)
            else:
                IsReady.append(True)
        except:
            IsReady.append(False)

    # Too early check the status of instances.
    # No InstanceStatuses is available.
    if len(IsReady) == 0:
        IsReady = [False] * len(InstanceIds)
    if len(IsReady) == 1:
        IsReady = IsReady[0]
#    assert len(IsReady) == len(InstanceIds)
    return (IsReady, InstanceIds)


def get_servers_state_by_InstanceIds(InstanceIds, debug=True):
    # Check mulitple instances by one query.
    # InstanceIds is a list of InstanceIds.
    # >>> True/False = check_servers_are_ready_by_InstanceIds('InstanceId')
    # >>> [True/False,...] = check_servers_are_ready_by_InstanceIds(['InstanceId',...])

    # Get status one by one
    # This is much simpler than all at the same time.
    # No sorting is needed.
    if type(InstanceIds) is str or type(InstanceIds) is unicode:
        InstanceIds = [InstanceIds]
    status_list = []
    for InstanceId in InstanceIds:
        status_list.append(get_server_state_by_InstanceId(InstanceId))
    return status_list


def get_server_state_by_InstanceId(InstanceId, debug=False):
    status = run_shell_check_output_json(
        'aws ec2 describe-instances --instance-ids ' + InstanceId, debug=debug)
    try:
        # "State"
        # 0 (pending), 16 (running), 32 (shutting-down), 48 (terminated), 64 (stopping), and 80 (stopped)
        # pending | running | shutting-down | terminated | stopping | stopped
        return status['Reservations'][0]['Instances'][0]['State']['Name']
    except:
        return None


def my_all(b_list):
    if type(b_list) is bool:
        return b_list
    else:
        return all(b_list)


def get_filenames(thedir):
    return [name for name in os.listdir(thedir) if os.path.isfile(os.path.join(thedir, name))]


def get_job_ids_from_filenames(fnames):
    return [os.path.splitext(fname)[0] for fname in fnames]


def terminate_servers_by_InstanceIds(InstanceIds, debug=True, notermination=False):
    if type(InstanceIds) is str or type(InstanceIds) is unicode:
        InstanceIds = [InstanceIds]
    IDs = " ".join(InstanceIds)
    output = ""
    if notermination:  # False, skipp this.
        output = run_shell_check_output_json \
            ("aws ec2 terminate-instances --instance-ids " + IDs, debug=debug)
    elif debug:
        print("aws ec2 terminate-instances --instance-ids " + IDs)
    return output


def get_my_InstanceId(debug=False):
    return run_shell_check_output("wget -q -O - http://instance-data/latest/"
                                  "meta-data/instance-id", debug=debug)


#def get_my_security_group_name(debug=False):
#    return run_shell_check_output("wget -q -O - http://instance-data/latest/"
#                                  "meta-data/security-groups", debug=debug)

def get_my_security_group_name(debug=False):
    """This is a hacky solution. There exists the discrepancy between awscli and the web (aws.amazon.com)."""
    return run_shell_check_output_json("aws ec2 describe-security-groups", debug=debug)['SecurityGroups'][0]['GroupName']




def get_my_security_group_id(security_group_name=None, debug=False):
    if security_group_name is None:
        security_group_name = get_my_security_group_name(debug=debug)
    output = run_shell_check_output_json('aws ec2 describe-security-groups '
                                         '--group-names ' + security_group_name, debug=debug)
    return output['SecurityGroups'][0]['GroupId']


def start_instance_by_InstanceId(InstanceId, debug=False):
    return run_shell_check_output_json("aws ec2 start-instances --instance-ids "
                                       + InstanceId)


def run_command_on_a_remote_server(IP, key_pair_path, mycommand):
    command = "ssh -q -oStrictHostKeyChecking=no -i " + key_pair_path +\
        " ubuntu@" + IP + " " + mycommand
    return run_shell_check_output(command)


def get_PublicIpAddress_by_InstanceId(InstanceId):
    try:
        info = run_shell_check_output_json(
            "aws ec2 describe-instances --instance-ids " + InstanceId)
        IP = info['Reservations'][0]['Instances'][0]['PublicIpAddress']
        return IP
    except:
        return None


def get_num_cpus_by_IntanceId(InstanceId, key_pair_path):
    IP = get_PublicIpAddress_by_InstanceId(InstanceId)
    return get_num_cpus(IP, key_pair_path)


def get_num_cpus(IP, key_pair_path):
    mycommand = "getconf _NPROCESSORS_ONLN"
    return int(run_command_on_a_remote_server(IP, key_pair_path, mycommand).strip())


def kill_all_jobs_on_server(IP, key_pair_path, binname):
    command = "ssh -q -oStrictHostKeyChecking=no -i " + key_pair_path + \
        " ubuntu@" + IP + ' killall -9 ' + binname
    run_shell(command)


def isIP(st):
    return re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', st) != None


def cleanup_node(IP, key_pair_path, debug=True):
    prefix = "ssh -q -oStrictHostKeyChecking=no -i " + key_pair_path + ' ubuntu@' + IP
    # Clean up only nonhidden folders and files on snodes (ubuntu@IP:~/)
    mycom = '\"rm -rf ~/*\"'
    try:
        command = prefix + ' ' + mycom
        print(command)
        output = run_shell_check_output(command, debug=True)
        if len(output) > 0:
            print(output)
    except:
        print('Error:' + command)
