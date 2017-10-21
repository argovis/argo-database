#!/bin/bash

echo 'Start of rsync and List'
DATE=`date +%y-%m-%d-%H:%M`
echo $DATE
FTPDIR='/storage/ifremer/'
#FTPDIR='/home/gstudent4/Desktop/ifremer/'
QUEUEDIR='/home/tyler/Desktop/argo/argo-database/queuedFiles/'
#QUEUEDIR='/home/gstudent4/Desktop/argo-database/queuedFiles/'
OUTPUTNAME=$QUEUEDIR'ALL-DACS-list-of-files-synced-'$DATE'.txt'
echo 'Starting rsync: writing to '$FTPDIR
#Sync only *_prof.nc
#rsync -arvzhim --delete --include='*/' --include='*_prof.nc' --exclude='*' vdmzrs.ifremer.fr::argo $FTPDIR > $OUTPUTNAME
#Sync only /profiles
#rsync -arvzhim --delete --include='**/' --include='**/profiles/**' --exclude='*' --exclude='**/profiles/B*' vdmzrs.ifremer.fr::argo/kordi $FTPDIR > $OUTPUTNAME
#Sync only /profiles/[RDM]*
rsync -arvzhim --delete --include='**/' --include='**/profiles/[RDM]*.nc' --exclude='*' --exclude='**/profiles/B*' vdmzrs.ifremer.fr::argo/coriolis $FTPDIR > $OUTPUTNAME
#Sync all .nc
#rsync -arvzhim --delete --include='*/' --include='*.nc' --exclude='*' vdmzrs.ifremer.fr::argo $FTPDIR > $OUTPUTNAME
ENDDATE=`date +%y-%m-%d-%H:%M`
echo 'End of rsync and List'
echo $ENDDATE
