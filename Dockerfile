FROM node:12.13.1 AS builder

WORKDIR /home/node

ARG BUILD_EXPIRE
ARG BUILD_DOMAIN
ARG REACT_APP_SENTRY_KEY=${REACT_APP_SENTRY_KEY:-""}
ARG REACT_APP_SENTRY_ORGANIZATION=${REACT_APP_SENTRY_ORGANIZATION:-""}
ARG REACT_APP_SENTRY_PROJECT=${REACT_APP_SENTRY_PROJECT:-""}

RUN npm i -g yarn

USER node

ENV REACT_APP_SENTRY_KEY=${REACT_APP_SENTRY_KEY}
ENV REACT_APP_SENTRY_ORGANIZATION=${REACT_APP_SENTRY_ORGANIZATION}
ENV REACT_APP_SENTRY_PROJECT=${REACT_APP_SENTRY_PROJECT}

COPY --chown=node:node package.json yarn.lock ./

RUN sed -i 's#https://registry.yarnpkg.com#https://registry.npmjs.org#g' yarn.lock \
    && yarn config set registry https://registry.npmjs.org \
    && yarn install --network-timeout 600000

COPY --chown=node:node . .

RUN ./scripts/build.sh

FROM nginx:mainline-alpine

COPY --from=builder /home/node/build /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 3000