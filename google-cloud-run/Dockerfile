FROM --platform=linux/amd64 node:16
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . ./
ENV PORT=8080
EXPOSE 8080
CMD ["node", "app.js"]