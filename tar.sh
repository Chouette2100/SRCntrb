#!/bin/bash
filename=`date +%Y%m%d-%H%M`
tar zcvf SRCntrb_$filename.tar.gz \
DBConfig.yml \
SRCntrb.00AC00 \
chgLM.sh
