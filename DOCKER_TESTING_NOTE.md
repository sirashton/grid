# IMPORTANT: Always Test in Docker Containers

**User has reminded me 3 times today to test everything in Docker containers.**

- All API tests must be run in Docker
- All database tests must be run in Docker  
- All gap detection tests must be run in Docker
- The application runs in Docker containers
- Local Python environment may not have the same dependencies or configuration

**Docker commands to use:**
- `docker-compose up -d` - Start containers
- `docker-compose exec grid-tracker python test_verification.py` - Run tests in grid-tracker container
- `docker-compose logs grid-tracker` - View logs
- `docker-compose down` - Stop containers
- `docker-compose ps` - Check running containers

**Service name is `grid-tracker`, not `app`** 