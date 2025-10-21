#!/bin/bash

s(){
 local digit=$1
 let res=0
 for ((i=0; i<=digit; i++))
 do
  res=$(($res+$i))
 done
 echo $res  
} 
  
factorial(){
 local num=$1
 let res=1
 for ((i=1; i<=num; i++))
  do
   res=$(($res*$i))
  done

 echo $res
}
read -p 'Enter ur number: ' numm
digit=numm
s numm
factorial digit  



