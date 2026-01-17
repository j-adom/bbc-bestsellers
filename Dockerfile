# Use Node.js LTS version
FROM node:18-alpine

# Set working directory
WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application files
COPY . .

# Expose the port (Coolify will handle port mapping)
EXPOSE 3001

# Start the application
CMD ["npm", "start"]
