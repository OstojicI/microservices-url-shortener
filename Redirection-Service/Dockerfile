FROM node:latest

COPY /src/package.json /nodeapp/src/package.json

WORKDIR /nodeapp/src

RUN npm install && mv /nodeapp/src/node_modules /nodeapp/node_modules

COPY /src /nodeapp/src

EXPOSE 3000

CMD npm start
