ARG BUILD_FROM
FROM $BUILD_FROM

WORKDIR /app
COPY package.json ./
RUN npm install --production
COPY *.js ./

CMD ["node", "index.js"]
