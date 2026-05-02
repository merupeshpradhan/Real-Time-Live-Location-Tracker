# Real-Time Live Location Tracker

## Project Overview
A real-time tracking system using Node.js, Socket.io, and Apache Kafka. Users authenticate via JWT, share their live GPS coordinates, and see other active users moving on an interactive Leaflet map.

## Tech Stack
- **Frontend**: Leaflet.js, Socket.io Client
- **Backend**: Node.js, Express.js, Socket.io
- **Message Broker**: Apache Kafka (KRaft mode)
- **Database**: Simulated via Kafka Consumer (`location_history.log`)
- **Authentication**: JWT (JSON Web Tokens)

## Setup Steps
1. **Start Kafka**: Run `docker-compose up -d`.
2. **Install Dependencies**: Run `pnpm install`.
3. **Setup Kafka Topic**: Run `node kafka-admin.js`.
4. **Start Backend**: Run `node --watch index.js `.
5. **Start DB Processor**: Run `node database-processor.js`.
6. **Access App**: Open `http://localhost:8000` in your browser.

## Event Flow
1. **Auth**: User logs in via `/login` -> receives JWT.
2. **Socket**: User connects to Socket.io with JWT -> Server verifies identity.
3. **Produce**: Frontend emits `client:location:update` -> Server publishes to Kafka topic `location-updates`.
4. **Consume (Broadcast)**: `index.js` consumes Kafka message -> Broadcasts to all connected users via Socket.io.
5. **Consume (Storage)**: `database-processor.js` consumes same Kafka message -> Appends to `location_history.log`.

## Demo Video
[Link to YouTube Video]

## Assumptions & Limitations
- **OIDC**: Simulated using a custom JWT login flow for local development.
- **Scaling**: Uses 2 Kafka partitions for basic parallelism.
- **Persistence**: Uses a log file instead of a heavy database for simplicity.
