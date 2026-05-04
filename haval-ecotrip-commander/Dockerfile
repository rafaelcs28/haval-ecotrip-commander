ARG BUILD_FROM
FROM $BUILD_FROM

RUN apk add --no-cache nodejs npm

WORKDIR /app
COPY package.json ./
RUN npm install --production
COPY *.js ./

# s6-overlay v3 service
RUN \
    mkdir -p /etc/s6-overlay/s6-rc.d/commander/dependencies.d \
    && echo "longrun" > /etc/s6-overlay/s6-rc.d/commander/type \
    && printf '#!/command/with-contenv sh\nexec node /app/index.js\n' \
         > /etc/s6-overlay/s6-rc.d/commander/run \
    && chmod +x /etc/s6-overlay/s6-rc.d/commander/run \
    && touch /etc/s6-overlay/s6-rc.d/commander/dependencies.d/base \
    && mkdir -p /etc/s6-overlay/s6-rc.d/user/contents.d \
    && touch /etc/s6-overlay/s6-rc.d/user/contents.d/commander
