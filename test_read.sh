#!/bin/bash
echo this is it
while read line
do
    echo $line
done < $final_pub_ip
