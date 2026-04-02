FROM node:22.17.1-alpine

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY package*.json ./
RUN npm install

# Copy application code
COPY . .

# Set build arguments
ARG NODE_ENV=production
ARG PORT=3000
ARG LOG_LEVEL=info

# Telemetry Processing Configuration Defaults
ARG BATCH_SIZE=500
ARG CRON_SCHEDULE="*/5 * * * *"
ARG LEADERBOARD_REFRESH_SCHEDULE="0 1 * * *"
ARG MV_REFRESH_SCHEDULE="*/15 * * * *"

# Set environment variables from build arguments
ENV NODE_ENV=$NODE_ENV
ENV PORT=$PORT
ENV LOG_LEVEL=$LOG_LEVEL
ENV BATCH_SIZE=$BATCH_SIZE
ENV CRON_SCHEDULE=$CRON_SCHEDULE
ENV LEADERBOARD_REFRESH_SCHEDULE=$LEADERBOARD_REFRESH_SCHEDULE
ENV MV_REFRESH_SCHEDULE=$MV_REFRESH_SCHEDULE

# Expose port
EXPOSE 3000

# Run the application (Production Mode)
CMD ["npm", "start"]

