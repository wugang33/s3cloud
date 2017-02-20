bucket=BUCKET-mygod  
url="host142"  
resource="/${bucket}/?prefix=&max-keys=1"  
contentType="application/x-compressed-tar"  
dateValue=`date -R -u`  
stringToSign="GET\n\n${contentType}\n${dateValue}\n/${bucket}/"  
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -v  -X GET -H "Host: ${url}" -H "Date: ${dateValue}" -H "Content-Type: ${contentType}"  -H "Authorization: AWS ${s3Key}:${signature}" "http://${url}${resource}"  
