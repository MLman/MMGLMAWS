#!/bin/sh
a=1
echo $slave_id_path
while read line 
do
#terminating all instances except master on CLI
echo aws ec2 terminate-instances --instance-ids "$line"> $log_path
#aws ec2 terminate-instances --instance-ids "$line"> $log_path
echo "Bringing down Machine #$a $line" > $log_path
echo "Bringing down Machine #$a $line"
a=$(($a + 1))
done < $slave_id_path
echo "We hope you enjoyed using this service"
