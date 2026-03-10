#!/bin/bash

# Nura Backend Production Deployment Script
# Usage: ./deploy.sh [option]
# Options: build, start, stop, restart, logs, status

set -e

PROJECT_NAME="nura_backend"
COMPOSE_FILE="docker-compose.prod.yml"

case "$1" in
    build)
        echo "Building production Docker image..."
        docker-compose -f $COMPOSE_FILE build --no-cache
        echo "✓ Build complete"
        ;;
    
    start)
        echo "Starting production containers..."
        docker-compose -f $COMPOSE_FILE up -d
        echo "✓ Containers started"
        echo "Waiting for health check..."
        sleep 10
        docker-compose -f $COMPOSE_FILE ps
        ;;
    
    stop)
        echo "Stopping production containers..."
        docker-compose -f $COMPOSE_FILE down
        echo "✓ Containers stopped"
        ;;
    
    restart)
        echo "Restarting production containers..."
        docker-compose -f $COMPOSE_FILE restart
        echo "✓ Containers restarted"
        ;;
    
    logs)
        echo "Showing logs (Ctrl+C to exit)..."
        docker-compose -f $COMPOSE_FILE logs -f
        ;;
    
    status)
        echo "Container status:"
        docker-compose -f $COMPOSE_FILE ps
        echo ""
        echo "Health status:"
        docker inspect --format='{{.State.Health.Status}}' nura_backend_prod 2>/dev/null || echo "Container not running"
        ;;
    
    deploy)
        echo "🚀 Deploying to production..."
        echo "1. Building image..."
        docker-compose -f $COMPOSE_FILE build --no-cache
        echo "2. Stopping old containers..."
        docker-compose -f $COMPOSE_FILE down
        echo "3. Starting new containers..."
        docker-compose -f $COMPOSE_FILE up -d
        echo "4. Waiting for startup..."
        sleep 15
        echo "5. Checking status..."
        docker-compose -f $COMPOSE_FILE ps
        echo "✓ Deployment complete!"
        ;;
    
    *)
        echo "Nura Backend Deployment Script"
        echo "Usage: $0 {build|start|stop|restart|logs|status|deploy}"
        echo ""
        echo "Commands:"
        echo "  build   - Build Docker image"
        echo "  start   - Start containers"
        echo "  stop    - Stop containers"
        echo "  restart - Restart containers"
        echo "  logs    - View container logs"
        echo "  status  - Show container status"
        echo "  deploy  - Full deployment (build + restart)"
        exit 1
        ;;
esac
