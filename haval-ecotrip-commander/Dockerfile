ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache nodejs npm

WORKDIR /app
COPY package.json ./
RUN npm install --production
COPY *.js ./

ENTRYPOINT ["node", "index.js"]
