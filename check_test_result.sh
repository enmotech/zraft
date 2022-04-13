#! /usr/bin/env bash

function print_ret(){
	msg=$1
	ret=$2
	if [[ ret -eq 0 ]];then
		echo $msg ": SUCCESS"
	else
		echo $msg ": FAILED"
		exit $ret
	fi
}

#make and run tests
function work(){
	#autoreconf
	if [ ! -e "./configure" ];then
		autoreconf -i
		print_ret "autoreconf" $?
	fi

	#update code
	git stash ./
	git checkout master
	git pull
	print_ret "update master" $?
	
	#clean
	if [ -e "./Makefile" ];then
		make clean 1>/dev/null 2>&1
		rm -f Makefile
	fi 
	print_ret "clean env" $?

	#configure
	./configure --disable-libtool-lock --disable-uv --enable-debug=yes --enable-sanitize=yes --enable-code-coverage 1>/dev/null 2>&1
	print_ret "configure" $?

	#make check
	make check 1>/dev/null 2>&1
	if [[ $? -eq 0 ]];then
		echo "compile run SUCCESS"
		exit 0
	else
		#TODO colloect logs
		echo "compile run FAIL"
		exit 1
	fi
}

#check tests result
work

