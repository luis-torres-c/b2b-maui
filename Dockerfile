FROM python:3.8

# Default setup
RUN apt-get update
RUN apt-get --assume-yes upgrade
# Package needed for monitoring data manually within container.
RUN apt-get install sqlite3
# Packages are needed by pymssql python lib. see requirements.txt
RUN apt-get --assume-yes install freetds-dev freetds-bin

ENV TZ=America/Santiago
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Packages from Selenium
ENV APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=DontWarn
RUN apt-get update
RUN apt-get --assume-yes install unzip xvfb libxi6 libgconf-2-4
RUN curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add
RUN echo "deb [arch=amd64]  http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list
RUN apt-get update && apt-get --assume-yes install google-chrome-stable
RUN wget https://chromedriver.storage.googleapis.com/102.0.5005.27/chromedriver_linux64.zip
RUN unzip chromedriver_linux64.zip -d /usr/src/app/
# Debian Buster (what this image is based on), the minimun tls version is 1.2
# Downgrade minimun tls version to 1.0 to support clients with old server configuration
RUN sed -i 's/TLSv1.2/TLSv1.0/g' /etc/ssl/openssl.cnf

# needed for pymssql build in python version 3.6.x or later
RUN export PYMSSQL_BUILD_WITH_BUNDLED_FREETDS=1

ENV APP_DIR=/usr/src/app/

# Add requirements.txt before rest of repo for caching
ADD requirements.txt $APP_DIR

WORKDIR $APP_DIR

# Upgrade default pip version to lastest
RUN pip install --upgrade pip

# Install libraries needed by the project.
RUN pip install -r requirements.txt

ADD . $APP_DIR

CMD ["python", "-u", "scheduler.py"]
