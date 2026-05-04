ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache nodejs npm

WORKDIR /app
COPY package.json ./
RUN npm install --production
COPY *.js ./

RUN mkdir -p /etc/services.d/commander
COPY run /etc/services.d/commander/run
RUN chmod +x /etc/services.d/commander/run
