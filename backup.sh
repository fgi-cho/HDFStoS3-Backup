#!/bin/bash
(
#
#      Create full/incremental backup of files from HDFS to A1 S3
#
#

function usage() {
cat << HELP
------------------------------------------------------------------------------------------------------------------------
${EXECUTABLE}
Purpose: Make Full or Incremental bachup of HDFS folder inclusive subfolders to A1-S3

Usage:   Configure argument list in bk_config.cfg

Argument list:
         HDFS_SOURCE_FOLDER      Mandatory: target folder in HDFS e.g. /prod/refined/.../X
         S3_BUCKET               Mandatory: S3 Bucket e.g. s3a//backup
         RETENTION_DAYS          Days keeping incremental backups
         FULL_BACKUP_DAY         On which day full backup is done

------------------------------------------------------------------------------------------------------------------------
HELP
}
# help options --help, -h or ?
if [ "x$1" = "x--help" -o "x$1" = "x-h" -o "x$1" = "x?" ]
l
then
    usage
    exit 97
fi

# ----------------------------------------------------------------------------------------------------------------------
# Import variables from config file
# ----------------------------------------------------------------------------------------------------------------------
if [ ! -f "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/bk_config.cfg ]
then
    usage
    echo "Config file is missing ... "
    exit 97
fi
source "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/bk_config.cfg
# ----------------------------------------------------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------------------------------------------------
# postfix for logfile name, etc.

LOCAL_TMP_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
dircount=1
date="$(date +%Y%m%d)"
AGO="`date --date "$DAYS_BEHIND days ago" "+%F %R"`"
FULL_DEL_AGO="`date --date "$FULL_RETENTION_DAYS days ago" "+%Y%m%d"`"
DEL_AGO="`date --date "$RETENTION_DAYS days ago" "+%Y%m%d"`"
echo "Source directory ${HDFS_SOURCE_FOLDER}"
echo "Destination directory $S3_BUCKET"
echo "Retention days $RETENTION_DAYS"
echo "Full backup should be done on $FULL_BACKUP_DAY every month"
echo "Data $(date +%d)"
echo "Incremental backup interval: $INCREMENTAL_INTERVAL"
echo "If weekly is selected incr. backup will be done every: $WEEK_DAY"
echo "Local tmp dir $LOCAL_TMP_DIR"

# ----------------------------------------------------------------------------------------------------------------------
# Check CLI parameters
# ----------------------------------------------------------------------------------------------------------------------
if [ ! "$(hdfs dfs -ls -d ${HDFS_SOURCE_FOLDER})" ]
then
    usage
    echo "HDFS_SOURCE_FOLDER missing!"
    exit 97
fi

if [ ! "$(s3cmd ls s3:/${S3_BUCKET}/)" ]
then
    usage
    echo "S3_BUCKET missing!"
    exit 97
fi
if [ "$(s3cmd ls s3:/${S3_BUCKET}/ | grep $date)" ]
then
    usage
    echo "Backup directory already exists"
    echo "Check for another running backup instance..."
    exit 97
fi

# ----------------------------------------------------------------------------------------------------------------------
# Cleaning old files
# ----------------------------------------------------------------------------------------------------------------------
    echo "Start cleanUp old tmp files ..."
    hdfs dfs -rm $TMP_DIR/filelist.txt
    hdfs dfs -rm $TMP_DIR/dirlist.txt
    hdfs dfs -rm -r $TMP_DIR/tmpdir
# ----------------------------------------------------------------------------------------------------------------------
# Incremental backup
# ----------------------------------------------------------------------------------------------------------------------

function incremental_bk() {

echo "# Will search for modified files whithin last $AGO days"
echo "# Will search under $HDFS_SOURCE_FOLDER"
    hdfs dfs -ls -R $HDFS_SOURCE_FOLDER | awk '$1 ~ /^[^d]/ && ($6 " " $7) > '"\"$AGO\""|awk -F" " '{print $6" "$7" "$8}'  | cut -d" " -f3  | hdfs dfs -appendToFile - $TMP_DIR/filelist.txt
echo "locate Directories"
    hdfs dfs -cat $TMP_DIR/filelist.txt | tail -n +2 | awk -F '/' '{$(NF--)=""; print}'| tr -s ' ' '/'|uniq | hdfs dfs -appendToFile - $TMP_DIR/dirlist.txt
echo " Copy files to local TMP directory"
    hdfs dfs -copyToLocal $TMP_DIR/filelist.txt $LOCAL_TMP_DIR
    hdfs dfs -copyToLocal $TMP_DIR/dirlist.txt $LOCAL_TMP_DIR
    mkdir -p $LOCAL_TMP_DIR/tmpdir/
 for dir in $(cat $LOCAL_TMP_DIR/dirlist.txt)
  do
   cat $LOCAL_TMP_DIR/filelist.txt | grep $dir > $LOCAL_TMP_DIR/tmpdir/dir$dircount.txt
    dircount=$((dircount+1))
 done
echo -e "\n Copy files to Hadood TMP directory ...\n"
    hdfs dfs -copyFromLocal $LOCAL_TMP_DIR/tmpdir/ $TMP_DIR
echo -e "\n Cleaning up local TMP folder...\n"
    rm -rf  filelist.txt dirlist.txt $LOCAL_TMP_DIR/tmpdir/
echo -e "\n Start files backup...\n"
 for cpfile in $( hdfs dfs -ls $TMP_DIR/tmpdir |tail -n +2 | awk '{print $NF}')
  do
    cpdir=$( hdfs dfs -cat $cpfile | tail -1 | awk -F '/' '{$(NF--)=""; print}'| tr -s ' ' '/')
    echo $cpfile
    echo $cpdir
    hadoop distcp -Dfs.s3a.access.key="JJVRWC2RQBP976BZAVCV" -Dfs.s3a.secret.key="r/xyeKuqnDCs1rWbj1Wsvc/VKLZdiAm/6tN/5rxt" -Dfs.s3n.multipart.uploads.enabled=true -Dfs.s3a.ssl.channel.mode=default_jsse_with_gcm -strategy dynamic -m 102 -update -pugpt -async -i -f $cpfile s3a:/$S3_BUCKET/Incremental_backup-$date/$cpdir
  done
echo -e "\n\nIncremental backup successfully completed ... "
echo -e "\nCleaning tmp files in $TMP_DIR/tmpdir ..."
 hdfs dfs -rm -r $TMP_DIR/tmpdir
}


# ----------------------------------------------------------------------------------------------------------------------
# Full backup
# ----------------------------------------------------------------------------------------------------------------------

function full_bk() {

echo -e "# \n\n\nStarting full backup "
    hadoop distcp -Dfs.s3a.access.key="JJVRWC2RQBP976BZAVCV" -Dfs.s3a.secret.key="r/xyeKuqnDCs1rWbj1Wsvc/VKLZdiAm/6tN/5rxt" -Dfs.s3n.multipart.uploads.enabled=true -Dfs.s3a.ssl.channel.mode=default_jsse_with_gcm -strategy dynamic -m 102 -update -pugpt -i $HDFS_SOURCE_FOLDER s3a:/$S3_BUCKET/Full_backup-$date/$cpdir
    echo -e "\n\n\nFull backup successfuly completed ..."
}


# ----------------------------------------------------------------------------------------------------------------------
# Retention/keeping incremental backup for XX days
# ----------------------------------------------------------------------------------------------------------------------

function retention() {
echo -e "\n\n Check for Old Incremental backups"
for lsdir in $( s3cmd ls s3://$S3_BUCKET/Incremental | awk  '{print $2}' )
  do
     INC_DEL=$( echo "$lsdir" | awk -F '/' '{print $(NF - 1)}' | awk -F '-' '{print $NF}' )
       if [[ "$INC_DEL" -lt "$DEL_AGO" ]]; then
         echo -e "\n\nDirectory $lsdir will be deleted..."
         echo "  Files Older that $RETENTION_DAYS days"
         s3cmd rm -r $lsdir
       fi
  done
echo -e "\n\n Check for Old Full backups...."
for fulllsdir in $( s3cmd ls s3://$S3_BUCKET/Full | awk  '{print $2}' )
  do
     FULL_INC_DEL=$( echo "$fulllsdir" | awk -F '/' '{print $(NF - 1)}' | awk -F '-' '{print $NF}' )
       if [[ $FULL_INC_DEL  -lt "$FULL_DEL_AGO" ]] && [ $"s3cmd ls s3://$S3_BUCKET/Full | wc -l" > 1 ];then
         echo -e "\n\nDirectory $fulllsdir will be deleted..."
         echo "  Files Older that $FULL_RETENTION_DAYS days"
         s3cmd rm -r $fulllsdir
       fi
  done
}

# ----------------------------------------------------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------------------------------------------------


if  [ "$(date +%d)" == "${FULL_BACKUP_DAY}" ]
then
   echo -e "\n\n\nStarting full backup ... "
   full_bk
else
   if [ "$INCREMENTAL_INTERVAL" == "daily" ]; then
     echo -e "\n\nStarting incremental backup ...\n"
     incremental_bk
   elif [ "$WEEK_DAY" == "$(date '+%A' | tr "[:upper:]" "[:lower:]")" ] && [ "$INCREMENTAL_INTERVAL" == "weekly" ]; then
     echo -e "\n\nStarting incremental backup ...\n"
     incremental_bk
   else
      echo -e "\n\nSkipping Incremental backup. Different interval range is set...\n"
   fi
fi

retention

) 2>&1 | tee backup-$(date +%Y%m%d).log
