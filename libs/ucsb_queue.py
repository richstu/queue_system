#!/usr/bin/env python
import subprocess
import queue_system
import os

class ucsb_queue(queue_system.queue_system):
  # Return command to run job for queue system
  # Should handle multiple commands in one job 
  # commands_info = [[command, job_index]]
  # submission_command_info = [submission_command, commands_info]
  def get_submission_command_info(self, commands_info, node):
    if os.environ['CMSSW_BASE'] == "": 
      print('[Error] CMSSW is not set')
      return []
    submission_command = 'JobSubmit.csh '
    if node:
      submission_command += '-node '+node+' '
    submission_command += os.environ['JB_QUEUE_SYSTEM_DIR']+'/bin/command_divider.py '
    for command, job_index in commands_info:
      command_with_env = os.environ['JB_QUEUE_SYSTEM_DIR']+'/bin/setcmsenv.sh '+os.environ['CMSSW_BASE']+' '+command
      #command_with_env = './setcmsenv.sh '+command.replace('"','\\"')
      submission_command += queue_system.compress_string(command_with_env)+' '
      #submission_command += './setcmsenv.sh '+command.replace('"','\\"')+'; '
    submission_command = submission_command[:-1]
    submission_command_info = [submission_command, commands_info]
    return submission_command_info

  # job_index starts from 1
  # commands_info = [[command, job_index]]
  # submission_command_info = [submission_command, commands_info]
  # Should modify jobs_info[job_index]['job_identifier']. Should modify jobs_info[job_index]['job_status'] to 'submitted'
  # jobs_info = ({'global_key':global_value},{'command': command for job 1, 'key_for_job':value_for_job1},{'command': command for job 2', key_for_job':value2_for_job2},...)
  def submit_job(self, submission_command_info, jobs_info):
    submission_command = submission_command_info[0]
    commands_info = submission_command_info[1]
    #print(commands_info)
    job_indices = self.get_job_indices_from_commands_info(commands_info)
    print('submit job_index(s): ' +str(job_indices)+' command: '+submission_command)

    job_result = subprocess.check_output(submission_command, shell=True)
    print(job_result)
    job_id = job_result.rstrip().replace('Submitted job ','')

    for multiple_index, job_index in enumerate(job_indices):
      jobs_info[job_index]['job_identifier'] = self.get_job_identifier(job_id,multiple_index)
      jobs_info[job_index]['job_status'] = 'submitted'
      print('[Info] Modified jobs_info['+str(job_index)+'] to '+job_id)
      print('[Info] Modified jobs_info['+str(job_index)+'] to submitted')

  # Should get job_index log using multiple_index and job_id.
  # Return 'not_found' if the file does not exists
  def get_job_log_string(self, job_id, multiple_index):
    log_path = '/net/cms2/cms2r0/'+os.getenv('USER')+'/log/'+job_id+'.log'
    log_string = 'not_found'
    if os.path.exists(log_path):
      # Divide according to below.
      # [Info] command_divider : Start divided_command[multiple_index]
      # [Info] command_divider : End divided_command[multiple_index]
      log_string = ''
      log_start = False
      log_end = False
      with open(log_path) as log_file:
        for line in log_file:
          if '[Info] command_divider : Start divided_command['+multiple_index+']' in line: log_start = True
          if '[Info] command_divider : End divided_command['+multiple_index+']' in line: log_end = True
          if log_start: log_string += line
          if log_end: break
      if log_start == False or log_end == False:
        log_string = "[Error] Couldn't find log_start: "+str(log_start)+" or log_end:"+str(log_end)+'\n'
        with open(log_path) as log_file:
          log_string += log_file.read()
    return log_string


if __name__ == '__main__':
  # Reserved keys: job_id, job_status
  # multiple jobs can have same job_id if they were submitted with one job command

  jobs_info_filename = 'jsons/mc_2016_jobs_info.json'
  out_jobs_info_filename = 'jsons/submitted_mc_2016_jobs_info.json'

  #jobs_info = datasets.load_json_file(jobs_info_filename)
  #queue = ucsb_job()
  #queue.submit_jobs_info(jobs_info, node='cms1')
  ##assign_job_id(queue, jobs_info, 6068)

  #datasets.save_json_file(jobs_info, out_jobs_info_filename)

