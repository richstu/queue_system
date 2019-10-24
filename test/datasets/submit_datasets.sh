# Enviornment
#   voms-proxy-init -voms cms -valid 168:0
#   source ../../set_env.sh

rm -rf jsons
rm -rf files
mkdir jsons
mkdir files

convert_cl_to_jobs_info.py ./command_list/cl_mc_dataset_files.py ./jsons/mc_jobs_info.json
auto_submit_jobs.py ./jsons/mc_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
check_jobs.py -s submitted,done,success,fail,to_submit jsons/auto_mc_jobs_info.json -c copy_aods_check_entries.py
auto_submit_jobs.py ./jsons/mc_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
select_resubmit_jobs.py jsons/auto_mc_jobs_info.json -c copy_aods_check_entries.py
auto_submit_jobs.py ./jsons/resubmit_auto_mc_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
echo Should get a failed job


convert_cl_to_jobs_info.py ./command_list/cl_data_dataset_files.py ./jsons/data_jobs_info.json
auto_submit_jobs.py ./jsons/data_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
auto_submit_jobs.py ./jsons/data_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
select_resubmit_jobs.py jsons/auto_data_jobs_info.json -c copy_aods_check_entries.py
auto_submit_jobs.py ./jsons/resubmit_auto_data_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
echo Should get a failed job


convert_cl_to_jobs_info.py ./command_list/cl_zjets_dataset_files.py ./jsons/zjets_jobs_info.json
auto_submit_jobs.py ./jsons/zjets_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
auto_submit_jobs.py ./jsons/zjets_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
select_resubmit_jobs.py jsons/auto_SIGNAL_NAME_jobs_info.json -c copy_aods_check_entries.py
auto_submit_jobs.py ./jsons/resubmit_auto_SIGNAL_NAME_jobs_info.json -n cms1 -c copy_aods_check_entries.py -t 10
echo Should get a failed job
