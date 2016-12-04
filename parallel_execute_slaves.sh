#!/bin/bash
echo "parallel_execute_slaves"
echo $slave_ip_path
echo `cat $slave_ip_path`
echo key_pair_path
echo $key_pair_path
while read line 
do
#these need to be done in parallel on the slave nodes
echo $line
#echo ssh -i ~/SECURITY_FILE/$key_pair_file ubuntu@$line 'bash -s < local.sh' $binpath $key_pair_file $myip $results_dir &
echo ssh -i $key_pair_path ubuntu@$line 'bash -s' < local.sh $binpath $key_pair_file $myip $results_dir &
ssh -i $key_pair_path ubuntu@$line 'bash -s' < local.sh $binpath $key_pair_file $myip $results_dir &
done < ${slave_ip_path}
#wait??
