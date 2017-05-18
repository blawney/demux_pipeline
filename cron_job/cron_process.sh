#!/bin/bash

# This is the script called in the crontab.  We need to add some environment variables and expose the 'software collections library' 
# so that the python virtual environment backing this process can be run.  Otherwise it cannot find various libraries needed to run.

# to use the virtualenv, need to enable software collections:
source /opt/rh/python27/enable 

# The next line updates PATH for the Google Cloud SDK for the gsutil commands.
if [ -f '/cbhomes2/cccbpipeline/google-cloud-sdk/path.bash.inc' ]; then source '/cbhomes2/cccbpipeline/google-cloud-sdk/path.bash.inc'; fi

# kickoff the scanner, which searches for unprocessed directories and subsequenctly calls another process to do everything
/ifs/labs/cccb/projects/cccb/pipelines/demux_and_delivery/cron_job/scan.py
