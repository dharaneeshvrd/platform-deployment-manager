#!/usr/bin/env python
import commands
import requests
import json

COMMAND_OUTPUT = commands.getoutput('yarn application -list')

IS_RUNNING = False

for line in COMMAND_OUTPUT.splitlines():
    fields = line.split('\t')
    if len(fields) >= 6:
        app = fields[1].strip()
        state = fields[5].strip()
        if app == '${component_job_name}': 
            IS_RUNNING = True
            yarn_app_id = fields[0].strip()
            tracking_url = fields[8].strip()
            break
if IS_RUNNING:
   url = '%s/jobs' % tracking_url
   flink_list_job_resp = requests.get(url)
   flink_list_job_resp = json.loads(flink_list_job_resp.text)
   flink_job_id = flink_list_job_resp['jobs-running'][0]
   commands.getoutput('flink cancel -yid %s %s' % (yarn_app_id, flink_job_id))
   commands.getoutput('yarn application -kill %s' % yarn_app_id)
