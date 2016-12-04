#!/bin/bash
#Usage ./execute_command_from_ipfile.sh final_public_ip.txt $key_pair_file
#See Also: main_script.sh

a=1
#cd ~/data
while read line 
do
        #The variable input data folder
	#echo "copying data partition folder"
	scp -q -i ~/SECURITY_FILE/$2 -r ~/data/run/$a ubuntu@$line:~/
	#echo "scp -q -i ~/SECURITY_FILE/$2 -r ~/data/run/$a ubuntu@$line:~/"
	#The security file for the copy back
	#echo "copying security file"
	scp -q -i ~/SECURITY_FILE/$2  ~/SECURITY_FILE/$2 ubuntu@$line:~
	#echo "scp -q -i ~/SECURITY_FILE/$2  ~/SECURITY_FILE/$2 ubuntu@$line:~"
	#The shared data folder common to all
	#echo "copying shared folder"
	scp -q -i ~/SECURITY_FILE/$2 -r ~/data/run/shared ubuntu@$line:~/
	#echo "scp -q -i ~/SECURITY_FILE/$2 -r ~/data/run/shared ubuntu@$line:~/"
	#The below line is not necessary, as we are executing commands remotely
	#scp -q -i ~/data/SECURITY_FILE/$2    remote.sh ubuntu@$line:~/
	#Making the slaves free of host checking for the copyback ;), it's secure and only for this particular master
	scp -q -i ~/SECURITY_FILE/$2 ~/.ssh/config_copy ubuntu@$line:~/.ssh/config
	#echo "scp -q -i ~/SECURITY_FILE/$2 ~/.ssh/config_copy ubuntu@$line:~/.ssh/config"
	
	#Look into implementing this feature
	#scp -q myfile user@host.com:. && echo success!
	echo "Completed transfer to Machine #$a @ $line---------->  Success"
	((a++))
done < "$1"
#cd ~/
