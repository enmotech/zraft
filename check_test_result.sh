#! /usr/bin/env bash

log_file='./test-suite.log'
test_suites=('paper_test' 'etcd_migrate')

function print_ret(){
	msg=$1
	ret=$2
	if [[ ret -eq 0 ]];then
		echo $msg ": SUCCESS"
	else
		echo $msg ": FAILED"
	fi
}

#make and run tests
function make_check(){
	#autoreconf
	if [ ! -e "./configure" ];then
		autoreconf -i
		print_ret "Autoreconf" $?
	fi

	#update code
	git restore ./
	git checkout master
	git pull
	print_ret "Swtich To Master And Update" $?
	

	#clean
	if [ -e "./Makefile" ];then
		make clean 1>/dev/null 2>&1
		rm -f Makefile
	fi 
	print_ret "Clean Environment" $?

	#configure
	./configure --disable-libtool-lock --disable-uv --enable-debug=yes --enable-sanitize=yes --enable-code-coverage 1>/dev/null 2>&1
	print_ret "Configure Enable Fixture And Debug" $?

	#make check
	make check 1>/dev/null 2>&1
	print_ret "Make & Run Tests" 0
}

#check tests result
function check_test_pass(){
	test_suite_name=$1
	if [ -e "./test-suite.log" ];then
		grep $test_suite_name $log_file
		test_cnt=`grep $test_suite_name $log_file | wc -l` 
		pass_cnt=`grep $test_suite_name $log_file | grep 'OK' | wc -l`

		if [ $test_cnt -eq $pass_cnt ];then
			return 0
		else
			return 1
		fi
	else
		exit 1
	fi
}

function work(){
	make_check

	echo "----------------------------------------Check Results---------------------------------------------"
	echo "--------------------------------------------------------------------------------------------------"

	fail_cnt=0
	for test_suite in ${test_suites[@]}; do
		check_test_pass $test_suite
		ret=$?
		fail_cnt=$[$ret+$fail_cnt]
	done

	if [[ fail_cnt -eq 0 ]];then
		exit 0
	else
		exit 1
	fi
}

work

