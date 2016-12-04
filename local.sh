#local.sh $JobId $binname $key_pair_file $myip $results_dir $raw_result_fname $job_done_dir
if [ "$#" -ne 7 ]
then
echo ">>> local.sh JobId exe_command key_pair_file myip results_dir raw_result_fname"
echo "raw_result_fname will be renamed to JobId.txt"
exit
fi
export OMP_NUM_THREADS=1

# Get the jobname
cd $1 
# Print my Public IP
myip="$(dig +short myip.opendns.com @resolver1.opendns.com)"
echo "My WAN/Public IP address: ${myip}" >> $1.log

echo "Starting job $1"  >> $1.log
#This is the command to run in a job directory with all shared files.
./$2 . .  >> $1.log
echo "./$2 . . "   in $1 >> $1.log


sleep 5

# post processing: rename the result file with JobId

#copy back p_value to master
echo "cp $6 $1.txt" >> $1.log
cp $6 $1.txt >> $1.log

echo "Copying back p_value result from the Machine for $1 to Master" >> $1.log

echo "scp -q -oStrictHostKeyChecking=no -i ~/$3 $1.txt ubuntu@$4:$5" >> $1.log
scp -q -oStrictHostKeyChecking=no -i ~/$3 $1.txt ubuntu@$4:$5 >> $1.log

ls -alFtr >> $1.log
echo done >> $1.log

#echo "scp -q -oStrictHostKeyChecking=no -i ~/$3 $1.log ubuntu@$4:$7"
scp -q -oStrictHostKeyChecking=no -i ~/$3 $1.log ubuntu@$4:$7

#echo "Shutdown the Machine for $a."
#sudo shutdown -h now
#check success of copying back somehow??
# Termination by itself?
exit
