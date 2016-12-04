#! /bin/sh
#secs=$((2 * 60))
#while [ $secs -gt 0 ]; do
#   echo -ne "$secs s remaining.\033[0K\r"
#   sleep 1
#   : $((secs--))
#   echo -ne "$secs s remaining..\033[0K\r"
#   sleep 1
#   : $((secs--))
#   echo -ne "$secs s remaining...\033[0K\r"
#   sleep 1
#   : $((secs--))
#done

secs=$((90))
while [ $secs -gt 0 ]; do
   #echo -ne "$secs s remaining.\033[0K\r"
   echo -ne "Waiting for the instances to boot up.   $secs s remaining.\033[0K\r"
   sleep 1
   : $((secs--))
   #echo -ne "$secs s remaining.\033[0K\r"
   echo -ne "Waiting for the instances to boot up..  $secs s remaining.\033[0K\r"
   sleep 1
   : $((secs--))
   #echo -ne "$secs s remaining.\033[0K\r"
   echo -ne "Waiting for the instances to boot up... $secs s remaining.\033[0K\r"
   : sleep 1
   $((secs--))
done

