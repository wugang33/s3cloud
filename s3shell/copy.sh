dst_bucket=BUCKET-mygod2 
src_bucket=BUCKET-mygod
objname="testfile"  
bucket=BUCKET-mygod  
resource="/${dst_bucket}/${objname}"  
contentType="application/x-compressed-tar"  
dateValue=`date -R -u`  
#x-amz-copy-source: ${src_bucket}/${objname}
stringToSign="PUT\n\n${contentType}\n${dateValue}\nx-amz-copy-source:/${src_bucket}/${objname}\n${resource}"  
s3Key="XN6QBMRYUE5H49KNVT1D"  
s3Secret="bo7ISjXTyrKElDkzLAsBCvXDvN22xh6e9VUA6qFS"  
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`  
curl -v -X PUT -H "Host: host142" -H "Date: ${dateValue}"  -H "x-amz-copy-source: /${src_bucket}/${objname}" -H "Content-Type: ${contentType}" -H "Authorization: AWS ${s3Key}:${signature}" "http://host142/${dst_bucket}/${objname}"  
