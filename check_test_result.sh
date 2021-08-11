#! /usr/bin/env bash

log_file='./test-suite.log'
test_suites=('paper_test' 'etcd_migrate')

#make and run tests
function make_check(){
	#configure
	if [ ! -e "./configure" ];then
		autoreconf -i
	fi

	#update code
	git restore ./
	git checkout master
	git pull

	#clean
	if [ -e "./Makefile" ];then
		make clean
		rm -f Makefile
	fi

	 ./configure --disable-libtool-lock --disable-uv --enable-debug=yes --enable-sanitize=yes --enable-code-coverage

	#make check
	make check 2>/dev/null
}

#check tests result
function check_test_pass(){
	test_suite_name=$1
	if [ -e "./test-suite.log" ];then
		test_cnt=`grep $test_suite_name $log_file | wc -l` 
		pass_cnt=`grep $test_suite_name $log_file | grep 'OK' | wc -l`

		if [ $test_cnt -eq $pass_cnt ];then
			echo $test_suite_name: PASS
			return 0
		else
			echo $test_suite_name: FAIL, total: $test_cnt, pass: $pass_cnt 
			return 1
		fi
	else
		exit 1
	fi
}

function work(){
	make_check

	echo "RESULT:"

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

