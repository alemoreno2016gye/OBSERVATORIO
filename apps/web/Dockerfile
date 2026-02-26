FROM node:20-alpine
WORKDIR /app/apps/web
COPY apps/web/package*.json ./
RUN npm install
COPY . /app
EXPOSE 3000
CMD ["npm", "run", "dev", "--", "--hostname", "0.0.0.0", "--port", "3000"]
