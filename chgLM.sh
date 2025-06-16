#!/bin/bash

nfn=`ls -tr SRCntrb.?????? | tail -1`; echo $nfn
nbe=$(basename "$nfn");echo $nbe

# systemctl --user stop SRCntrb
rm SRCntrb
ln -s ./$nbe SRCntrb
ls -l
# systemctl --user start SRCntrb
# systemctl --user status SRCntrb
