
#verify(p_values)
cd ~/P_VALUE

count=`ls | wc -l`

#Check if 50 files produced-only then terminate
#keeps looping until count hits 50 || yay
#can implement a timeout based straggler handler
while [ $count -lt 5 ]
do
temp=$((5-$count))
echo -ne "Waiting on $temp more value sets "
count=`ls | wc -l`
#echo -ne "Processing.\033[0K\r"
#sleep 2
#echo -ne "Processing..\033[0K\r"
#sleep 2
#echo -ne "Processing...\033[0K\r"
#sleep 2
#echo -ne "Processing.\033[0K\r"
#sleep 2
#echo -ne "Processing..\033[0K\r"
#sleep 2
#echo -ne "Processing...\033[0K\r"
#sleep 2
#echo -ne "Processing.\033[0K\r"
#sleep 2
#echo -ne "Processing..\033[0K\r"
#sleep 2
#echo -ne "Processing...\033[0K\r"
sleep 20 #checks every 20s if job completed
done
