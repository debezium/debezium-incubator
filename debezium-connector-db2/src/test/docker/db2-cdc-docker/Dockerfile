FROM ibmcom/db2

MAINTAINER  Peter Urbanetz


RUN mkdir -p /asncdctools/src

ADD asncdc_UDF.sql /asncdctools/src
ADD asncdcaddremove.sql /asncdctools/src
ADD asncdctables.sql /asncdctools/src
ADD dbsetup.sh /asncdctools/src
ADD asncdc.c /asncdctools/src
ADD bldrtn /asncdctools/src


RUN chmod -R  777  /asncdctools

RUN mkdir /var/custom
RUN chmod -R  777 /var/custom

ADD cdcsetup.sh /var/custom