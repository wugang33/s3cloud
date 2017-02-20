file="./myfile"  
objname="testfile"  
bucket=BUCKET-mygod  
url="host142"  
resource="/"   
dateValue=`date -R -u`  
stringToSign="GET\n\n\n${dateValue}\n${resource}"  
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -v -o ${file} -X GET -H "Host: ${url}" -H "Date: ${dateValue}"  -H "Authorization: AWS ${s3Key}:${signature}" "http://${url}/"  
