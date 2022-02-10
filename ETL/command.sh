#!/bin/sh

# Execute Script (Collect more data)
/Users/nando/.pyenv/versions/3.9.2/envs/dsaodevenv/bin/python /Users/nando/Comunidade\ DS/ds_ao_dev/ETL/script.py

# Git Add
cd /Users/nando/Comunidade\ DS/ds_ao_dev/ && /opt/homebrew/bin/git add . 

# Git Commit
cd /Users/nando/Comunidade\ DS/ds_ao_dev/ && /opt/homebrew/bin/git commit -m "updating database automatically using crontab"

# Git Push
cd /Users/nando/Comunidade\ DS/ds_ao_dev/ && /opt/homebrew/bin/git push origin master