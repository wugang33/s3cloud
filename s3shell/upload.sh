file=./c.zip  
objname="testfile"  
bucket=BUCKET-mygod  
resource="/${bucket}/${objname}"  
contentType="application/x-compressed-tar"  
dateValue=`date -R -u`  
stringToSign="PUT\n\n${contentType}\n${dateValue}\n${resource}"  
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -X PUT -T "${file}" -H "Host: host142" -H "Date: ${dateValue}" -H "Content-Type: ${contentType}" -H "Authorization: AWS ${s3Key}:${signature}" "http://host142/${bucket}/${objname}"  
