FROM node:20-alpine

WORKDIR /app

COPY package.json ./

RUN npm install --omit=dev

COPY server.mjs ./

ENV NODE_ENV=production

EXPOSE 8090

CMD ["node", "server.mjs"]