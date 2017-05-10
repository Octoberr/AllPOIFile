#coding:utf-8

import os
filedir="F:\\busdata\\tiehang"

for parent,dirname,filenames in os.walk(filedir):
    for filename in filenames:
        # print "the full name of the file is:" + os.path.join(parent, filename.decode("utf-8"))

        print filename.encode("utf8")