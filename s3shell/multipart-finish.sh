#PUT /BUCKET-mygod/c.zip?partNumber=1&uploadId=2~91zJMtX2mYUSqtrhHw2lfkKGm_RhQAM
file=./finish.xml
uploadid=$1
echo $uploadid 
objname="testfile-not-exists"  
bucket=BUCKET-mygod  
resource="/${bucket}/${objname}?uploadId=${uploadid}"  
contentType="application/octet-stream"  
dateValue=`date -R -u`  
stringToSign="POST\n\n${contentType}\n${dateValue}\n${resource}"  
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -v -X POST  -T "${file}" -H "Host: host142" -H "Date: ${dateValue}" -H "Content-Type: ${contentType}" -H "Authorization: AWS ${s3Key}:${signature}" "http://host142${resource}"  
