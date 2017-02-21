#!/bin/bash

# to use the virtualenv, need to enable software collections:
source /opt/rh/python27/enable 

# kickoff the scanner, which searches for unprocessed directories and subsequenctly calls another process to do everything
/ifs/labs/cccb/projects/cccb/pipelines/demux_pipeline_current/cron_job/scan.py
