# /bin/sh
#Author - Hyunwoo J. Kim, UW-Madison
#Author - Anmol Mohanty, UW-Madison

# Structure in the master server.
#/home/ubuntu/SCRIPTS   :  bash scripts
#/home/ubuntu/data/run  :  Data
#/home/ubuntu/PVAL_OUT  :  pvalue out
#/home/ubuntu/SECURITY_FILE : key pair

# Structure in the slave servers.
#/home/ubuntu/shared/idx_perm_arma.mat   
#/home/ubuntu/shared/mmglm_spd_par
#/home/ubuntu/shared/Xs_arma.mat
#/home/ubuntu/1/mask_job_arma.mat  
#/home/ubuntu/1/Ys_arma.mat
#/home/ubuntu/hyunwoo_key.pem

# Remotely run jobs by local.sh $binpath $key_pair_file $myip

cd ~/SCRIPTS
./print_welcome.sh

## Read option
echo Read Option
echo ./option.sh
. ./option.sh

## Clean 
#echo ./clean_start.sh
#. ./clean_start.sh


# Spawn servers as many as the number of jobs
echo""
#echo ./spwan_servers.sh
#. ./spawn_servers.sh

# Get server information
echo ./get_server_info.sh
. ./get_server_info.sh
#Check all servers are running. STATE RUNNING servers running state

# Pull up the list of ips and connects to them seamlessly
# and copies over various portions of data

## Scheduler. Save current status
## nservers_in_pool

echo ./transfer_files_to_slaves $slave_ip_path $key_pair_path $input_dir $job_ip_tab_path
./transfer_files_to_slaves $slave_ip_path $key_pair_path $input_dir $job_ip_tab_path
#./execute_command_from_ipfile.sh $tmp_dir/$slave_ip_fname $key_pair_path $input_dir $job_ip_tab_path
## Execute individual queries (p_value computations) on slaves parallely
. ./parallel_execute_slaves.sh
exit
## Polling, check how many jons are done. By default, every 10 secs.
## Termination for too long jobs... How?

#### ./check_jobs ###

# Terminating slave servers
echo . ./terminate.sh 
. ./terminate.sh 
exit

#Check all servers are running.
#STATE RUNNING servers running state

## Polling, check how many jons are done. By default, every 10 secs.
## Termination for too long jobs... How?
#./check_jobs

#begin instance termination 
#Iterate through the ids and terminate instances
#./terminate.sh  final_ids.txt > silence_terminate_log.txt &
./terminate.sh  $slave_ip_fname > $tmp_dir/termination_log.txt &

echo . ./clean_end_.sh
. ./clean_end_.sh
echo ./goodbye.sh
./goodbye.sh

