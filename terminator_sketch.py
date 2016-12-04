from os.path import expanduser
from aws_tools import *
slave_ip_path = "~/temp/final_public_ip.txt"
key_pair_path = "~/SECURITY_FILE/hyunwoo_key.pem"
ReservationId = "r-2650fa84"
#./save_PublicIpAddresses_by_ReservationID $ReservationId $server_info_path $slave_ip_path
fname = "/home/ubuntu/temp/servers.txt"
IP_N_IDs = get_PubIpAddr_N_InsId_by_ReservationId(fname, ReservationId)
input_dir = "/home/ubuntu/data/run_1"


# Build job list
from scheduler import *
input_dir = expanduser("~/data/run_1/")
jobs = init_jobs(input_dir)

# JSON encoding and decoding
import json
scheduler_info_path = expanduser("~/temp/scheduler_info.txt")
write_json_file(scheduler_info_path, jobs)
jobs_recovered = read_json_file(scheduler_info_path)

#[job list]
# Run all jobs are completed.

#[node list]
# Check pvalue are copied.
# Check the status of node
# Copied && stopped => Terminate

# Run node until it completed
# Verify result.
# Copy location
# Terminate the nocde.
