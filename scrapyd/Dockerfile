FROM python:3.6.9

# install Ubuntu packages
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update -qq \
 && apt-get install --no-install-recommends -y \
    git nginx apache2-utils \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# install Chaperone
# (Unofficial repo due to: https://github.com/garywiz/chaperone/issues/24)
#RUN pip3 install chaperone \
RUN pip3 install git+https://github.com/necrophcodr/chaperone.git \
 && mkdir /etc/chaperone.d

ADD requirements.txt /
RUN pip3 install -r /requirements.txt \
 && pip3 freeze > /pip3-freeze.txt

# configure
ADD chaperone.conf /etc/chaperone.d/chaperone.conf
ADD nginx.conf /etc/nginx/sites-enabled/default
ADD scrapyd.conf /etc/scrapyd/scrapyd.conf

# expose
VOLUME /scrapyd
EXPOSE 6800

ENTRYPOINT ["/usr/local/bin/chaperone"]