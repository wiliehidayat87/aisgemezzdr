#!/bin/bash
while :
do
	dd=`date --date='1 day ago' +%Y%m%d`
    sudo scp -rp cpuser880@202.149.24.122:/nas-droffline/cpuser880/4880456_$dd* /xmp/th/aisgemezzdr/data/
    sudo scp -rp cpuser880@202.149.24.122:/nas-droffline/cpuser880/4880457_$dd* /xmp/th/aisgemezzdr/data/
    sudo scp -rp cpuser880@202.149.24.122:/nas-droffline/cpuser880/4880458_$dd* /xmp/th/aisgemezzdr/data/

    #error_notification_file_4880456_

    sudo scp -rp cpuser880@202.149.24.122:/nas-droffline/cpuser880/error_notification_file_4880456_$dd* /xmp/th/aisgemezzdr/data/
    sudo scp -rp cpuser880@202.149.24.122:/nas-droffline/cpuser880/error_notification_file_4880456_$dd* /xmp/th/aisgemezzdr/data/
    sudo scp -rp cpuser880@202.149.24.122:/nas-droffline/cpuser880/error_notification_file_4880456_$dd* /xmp/th/aisgemezzdr/data/

    #error_notification_file_4880456_

	sleep 1m
done
