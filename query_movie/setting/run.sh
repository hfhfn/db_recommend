#!/bin/bash

cd `$dirname`|exit 0
#source activate ds
source activate sklearn
python3 check_article_server.py
