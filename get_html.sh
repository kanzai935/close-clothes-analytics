#!/bin/bash

category=$1
page=$2
url="http://zozo.jp/category/${category}/?pno=${page}\&dord=21\&dstk=2"
userAgent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36"
dir="/Users/kumiko/alibaba-image/"
fileName=`date +%Y%m%d%H%M`

# crawling
wget ${url} -O ${dir}${fileName} -U ${userAgent}

# convert to utf-8
iconv -f SJIS -t UTF8 ${dir}${fileName} > ${dir}${fileName}.html

# upload s3 bucket
/usr/local/bin/aws s3 cp ${dir}${fileName}.html s3://zozo-image-html/${category}/

# delete html
rm -f ${dir}${fileName}
rm -f ${dir}${fileName}.html
