#!bin/sh
a=1
while [ $a -le 20 ]
do
mv $a junk/
((a++))
done
