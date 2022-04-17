FROM public.ecr.aws/lambda/python:3.8
COPY requirements.txt  .
RUN yum install -y postgresql-devel \
    && pip3 install --no-cache-dir -r requirements.txt --target "${LAMBDA_TASK_ROOT}" \
    && yum clean all \
    && rm -rf /var/cache/yum
COPY scheduler.py ${LAMBDA_TASK_ROOT}
COPY scraper_job.py ${LAMBDA_TASK_ROOT}
CMD [ "scheduler.handler" ]
