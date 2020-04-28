FROM python:3.8

# Docker build command
# docker build --build-arg environment=dev -t ds-border-cross:latest .

# Create app directory
WORKDIR /app

# Install app dependencies
#COPY ./requirements.txt .
#RUN pip install -r requirements.txt
RUN pip install pytest
RUN pip install pytest-cov

# Pass the environment during build.
#ARG environment
#ENV BORDER_CROSS_ENV=$environment

# Bundle app source
COPY ./ .

#EXPOSE 8080
CMD [ "/app/test_coverage.sh" ]
