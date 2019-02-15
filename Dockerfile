FROM node:8.15-jessie

ENV NODE_ENV=production
ENV PORT=5000
EXPOSE 5000
# TODO: Pull the whole src, either locally or from git, and then build
COPY package.json .
COPY build/index.js .
RUN yarn
CMD node index.js