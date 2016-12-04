echo "-------------Get Information of servers----------------------"
# Get ReservationId
echo ReservationId=./get_ReservationId $log_path
ReservationId=`./get_ReservationId $log_path`
echo ReservationId: $ReservationId

#sleep 60 # If possible, let us remove it. Internally checking until all servers' information are available.
server_info_fname=servers.txt
server_info_path=$tmp_dir/servers.txt
slave_ip_fname=final_public_ip.txt
slave_ip_path=$tmp_dir/final_public_ip.txt
slave_id_fname=ids.txt
slave_id_path=$tmp_dir/ids.txt
aws ec2 describe-instances > $server_info_path

# Get list of instance ids handy for emergency termination if necessary
echo ./save_InstanceIds_by_ReservationId $ReservationId $server_info_path $slave_id_path
./save_InstanceIds_by_ReservationId $ReservationId $server_info_path $slave_id_path
echo -e "\n===Slave nodes IDs===="
cat $slave_id_path

# Get Public IPs of slave nodes in the reservation.
echo ./save_PublicIpAddresses_by_ReservationID $ReservationId $server_info_path $slave_ip_path
./save_PublicIpAddresses_by_ReservationID $ReservationId $server_info_path $slave_ip_path
echo -e "\n===Slave nodes IPs==="
cat $slave_ip_path

#look into making this dynamic as well - could be interesting
myip="$(dig +short myip.opendns.com @resolver1.opendns.com)"
echo -e "\nMy WAN/Public IP address: ${myip}"
