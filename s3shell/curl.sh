bucket="BUCKET-mygod"  
#dateValue=`date -R -u`  
dateValue="Thu, 09 Feb 2017 09:28:59 GMT"
resource="/${bucket}/"  
contentType="application/octet-stream"  
stringToSign="PUT\n\n\n${dateValue}\n${resource}"  
echo ${stringToSign}
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -v -X PUT "http://host142/${bucket}/" -H "Host: host142" -H "Date: ${dateValue}" -H "Authorization: AWS ${s3Key}:${signature}"  
