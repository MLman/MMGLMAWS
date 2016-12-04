# /bin/sh
#Author - Hyunwoo J. Kim, UW-Madison

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

cd ~/SCRIPT
./print_welcome.sh 
date

## Read option
echo Read Option
echo ./option.sh
. ./option.sh

## Clean 
echo ./clean_start.sh
. ./clean_start.sh

## Write option in a JSON file.
echo $'\n' WRITE OPTION JSON FILE at $tmp_dir/option.json $'\n'
. ./write_option_json.sh

# Start of scheduler 
echo ./run_scheduler $tmp_dir/option.json
./run_scheduler $tmp_dir/option.json

echo . ./clean_end.sh
#. ./clean_end.sh
echo ./goodbye.sh
./goodbye.sh
date

