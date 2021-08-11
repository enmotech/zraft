#! /usr/bin/env bash

echo "check raft test result:"

log_file='./test-suite.log'
test_suites=('paper_test' 'etcd_migrate')

function check_test_pass(){
	test_suite_name=$1
	if [[ -f "./test-suite.log" ]];then
		test_cnt=`grep $test_suite_name $log_file | wc -l` 
		pass_cnt=`grep $test_suite_name $log_file | grep 'OK' | wc -l`

		if [[ test_cnt -eq pass_cnt ]];then
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



