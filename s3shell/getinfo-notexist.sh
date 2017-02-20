file="./myfile"  
objname="testfile-not-exist"  
bucket=BUCKET-mygod  
url="host142"  
resource="/${bucket}/${objname}"  
contentType="application/x-compressed-tar"  
dateValue=`date -R -u`  
stringToSign="HEAD\n\n${contentType}\n${dateValue}\n${resource}"  
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -v -o ${file} -X HEAD -H "Host: ${url}" -H "Date: ${dateValue}" -H "Content-Type: ${contentType}"  -H "Authorization: AWS ${s3Key}:${signature}" "http://${url}/${bucket}/${objname}"  
