#!/usr/bin/env python
import sys
import nested_dict
import subprocess
import ucsb_queue
import time
import queue_system
import os
import argparse
import ask
# This script depends on the queue system

def which(program):
  def is_exe(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
  fpath, fname = os.path.split(program)
  if fpath:
    if is_exe(program):
      return program
  else:
    for path in os.environ["PATH"].split(os.pathsep):
      exe_file = os.path.join(path, program)
      if is_exe(exe_file):
        return exe_file
  return None

def initialize_arguments(args):
  for key in args:
    if isinstance(args[key], list) and len(args[key])==1: 
      args[key] = args[key][0]
      if unicode(args[key]).isnumeric():
        args[key] = int(args[key])
  if not args['output_json']:
    folder = os.path.dirname(args['jobs_info_filename'])
    filename = os.path.basename(args['jobs_info_filename'])
    args['output_json'] = os.path.join(folder,'auto_'+filename)
  if not args['max_trials']: args['max_trials'] = 10
  if not args['pause_time']: args['pause_time'] = 60
  if not args['jobscript_check_filename']: args['jobscript_check_filename'] = 'jobscript_check.py'

def are_arguments_valid(args):
  if not os.path.isfile(args['jobs_info_filename']):
    return False, 'jobs_info_filename: '+args['jobs_info_filename']+" doesn't exist."

  if os.path.isfile(args['output_json']):
    overwrite = ask.ask_key(args['output_json']+' already exits. Do you want to overwrite? (y/n) Default is n. ', ['y','n'], 'n')
    if overwrite == 'n':
      return False, 'output_json: '+args['output_json']+' already exits.'

  #if not os.path.isfile(args['jobscript_check_filename']):
  #  return False, 'jobscript_check_filename: '+args['jobscript_check_filename']+" doesn't exist."

  if not which(args['jobscript_check_filename']):
    return False, 'jobscript_check_filename: '+args['jobscript_check_filename']+" isn't executable."

  return True, ''

if __name__ == '__main__':

  parser = argparse.ArgumentParser(description='Submit jobs using jobs_info.json')
  parser.add_argument('jobs_info_filename', metavar='jobs_info.json')
  parser.add_argument('-o', '--output_json', metavar='auto_jobs_info.json', nargs=1)
  parser.add_argument('-n', '--node', metavar='"node_name"', nargs=1)
  parser.add_argument('-m', '--max_trials', metavar=10, nargs=1)
  parser.add_argument('-t', '--pause_time', metavar=60, nargs=1)
  parser.add_argument('-c', '--jobscript_check_filename', metavar='jobscript_check.py', nargs=1)
  parser.add_argument('-f', '--force_run', action='store_true')
  parser.add_argument('-j', '--jobs_to_combine', metavar='None', nargs=1)
  args = vars(parser.parse_args())

  initialize_arguments(args)
  valid, log = are_arguments_valid(args)
  if not valid:
    print('[Error] '+log)
    sys.exit()

  #jobs_info_filename = 'jsons/auto_fix_checked_mc_2016_jobs_info.json'
  #output_json = 'jsons/auto_fix_checked_mc_2016_jobs_info.json'
  #jobscript_check_filename = './copy_aods_check_entries.py'
  #max_trials = 10
  #pause_time = 60
  #node = 'cms1'

  jobs_info_filename = args['jobs_info_filename']
  output_json = args['output_json']
  jobscript_check_filename = args['jobscript_check_filename']
  max_trials = args['max_trials']
  pause_time = args['pause_time']
  node = args['node']

  jobs_info = nested_dict.load_json_file(jobs_info_filename)
  #nested_dict.save_json_file(jobs_info, output_json)
  #jobs_info = nested_dict.load_json_file(output_json)

  queue = ucsb_queue.ucsb_queue()
  if args['force_run']:
      nJobs = queue.get_number_jobs(jobs_info, ['to_submit'])
      if args['jobs_to_combine']: number_combined_commands = int(args['jobs_to_combine'])
      elif nJobs>300: number_combined_commands = nJobs/300
      else: number_combined_commands = nJobs
      print_or_run = 'r'
      queue.raw_submit_jobs_info(jobs_info, node, number_combined_commands, print_or_run)
  else:
    # First submit of jobs
    # jobs_info = [{'command_script':command_script, 'other_global_key':other_global_key},{'key_for_job':key_for_job},{'key_for_job':key_for_job},...]
    # statuses: [status], where status = 'submitted', 'done', 'fail', 'success', 'to_submit'
    node, number_combined_commands, print_or_run  = queue.submit_jobs_info(jobs_info, node=node)
    if print_or_run == 'p': 
      sys.exit()
  nested_dict.save_json_file(jobs_info, output_json)

  number_submitted = queue.get_number_jobs(jobs_info, ['submitted'])
  while number_submitted != 0:
    print('[Info] Pausing '+str(pause_time)+' sec')
    time.sleep(pause_time)

    queue.check_jobs(jobs_info, ['submitted'], jobscript_check_filename)
    #queue.add_trials_jobs(jobs_info, ['to_submit'])
    queue.fail_max_trials_jobs(jobs_info, ['to_submit'], max_trials)
    nested_dict.save_json_file(jobs_info, output_json)

    print('[Info] Before submit')
    queue.print_jobs_status(jobs_info)

    queue.raw_submit_jobs_info(jobs_info, node, number_combined_commands, print_or_run)

    print('[Info] After submit')
    queue.print_jobs_status(jobs_info)

    number_submitted = queue.get_number_jobs(jobs_info, ['submitted'])

  # Give error if there is a fail
  number_fail = queue.get_number_jobs(jobs_info, ['fail'])
  if number_fail != 0: 
    sys.exit(1)
