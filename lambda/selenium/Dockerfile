FROM public.ecr.aws/lambda/python:3.8 as base

COPY install-browser.sh /tmp/

RUN yum install -y -q unzip https://dl.google.com/linux/direct/google-chrome-stable_current_x86_64.rpm \
    && /usr/bin/bash /tmp/install-browser.sh \
    && pip install --upgrade pip -q \
    && pip install selenium \
    && yum clean all \
    && rm -rf /var/cache/yum

COPY . ./
CMD [ "selenium_handler.lambda_handler" ]
