uploadId=$1
objname="testfile-not-exists"  
bucket=BUCKET-mygod  
resource="/${bucket}/${objname}?uploadId=${uploadId}"  
contentType="application/octet-stream"  
dateValue=`date -R -u`  
stringToSign="DELETE\n\n${contentType}\n${dateValue}\n${resource}"  
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -v -X DELETE  -H "Host: host142" -H "Date: ${dateValue}" -H "Content-Type: ${contentType}" -H "Authorization: AWS ${s3Key}:${signature}" "http://host142${resource}"  
