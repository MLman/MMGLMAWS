#! /bin/sh
#Author - Hyunwoo J. Kim, UW-Madison

# Basic options
key_pair_file=hyunwoo_key.pem
#server_type=t2.micro # or c4.8xlarge
server_type=c4.8xlarge
input_dir=~/data/run_aws_ni_test_2vox
results_dir=~/PVALUE_aws_ni_test_2vox

#input_dir=~/data/run_aws_ni_exp_01
#results_dir=~/P_VALUE_aws_ni_exp_01
raw_result_fname=p_value.txt
max_num_servers=2
binpath=mmglm_spd

# Options for advanced users
tmp_dir=~/temp
logpath=$tmp_dir/log.txt
option_json=$tmp_dir/option.json
job_done_dir=$tmp_dir/job_done
debug_level=1 # 0 (minimum) | 1 (some) | 2 (detailed)
notermination=false # NOTermiatnion. Keep machines running.

mkdir -p $tmp_dir
mkdir -p $job_done_dir
mkdir -p ~/junk # For old log
mkdir -p $results_dir
local_run_script=local.sh
image_id=ami-fce3c696
security_group_ids="" # (default) security_group_id of Master Server
#sInstanceIds="i-b17a1821,i-b27a1822" # (default) empty. This is for debugggin. No space
sInstanceIds="" # (default) empty. This is for debugggin. No space, separated by commas
key_pair_name=${key_pair_file%.*}
key_pair_path=~/SECURITY_FILE/$key_pair_file
chmod 600 $key_pair_path # For overwriting
ndirs=$(find ${input_dir}/* -maxdepth 0 -type d | wc -l)
# The shared directory does not count.
njobs=`expr $ndirs - 1`
job_ip_tab_file=job_ip_table.txt
job_ip_tab_path=$tmp_dir/$job_ip_tab_file
max_num_servers=$(($max_num_servers<$njobs?$max_num_servers:$njobs))
master_instanceid=`wget -q -O - http://instance-data/latest/meta-data/instance-id`
log_change_only=true # true | false
echo '\n'"### OPTIONS ###"'\n'
echo Input Directory = $input_dir
echo Executable file = $binpath
echo Key pair file = $key_pair_file
echo Key pair file path = $key_pair_path
echo Master Server InstanceId=$master_instanceid
echo Maximum number of Servers "= min(max_num_servers, njobs)" = $max_num_servers 

# For schedular
scheduler_info_path=$tmp_dir/scheduler_info.json
scheduler_log_path=$tmp_dir/scheduler_info_log.json
