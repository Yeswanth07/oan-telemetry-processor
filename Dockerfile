FROM node:22.17.1-alpine

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
COPY package*.json ./
RUN npm install

# Copy application code
COPY . .

# Set environment variables
ENV NODE_ENV=production
ENV PORT=3000
ENV LOG_LEVEL=info

# Telemetry Processing Configuration Defaults
ENV BATCH_SIZE=500
ENV CRON_SCHEDULE="*/5 * * * *"
ENV LEADERBOARD_REFRESH_SCHEDULE="0 1 * * *"
ENV MV_REFRESH_SCHEDULE="*/15 * * * *"

# Expose port
EXPOSE 3000

# Run the application (Production Mode)
CMD ["npm", "start"]

