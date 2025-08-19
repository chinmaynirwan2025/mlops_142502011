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

s numm
