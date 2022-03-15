# URL shortener - intro to microservices
You can read complete article on my [Medium page](https://ivanostojic.medium.com/microservices-for-beginners-developing-simple-url-shortener-2e6fab5c473e)

Used technologies: 
- Node.js
- Docker
- RabbitMQ
- Redis
- MySQL

If you just want to start the application run:

    docker-compose up
It is possible that one of the containers is going to fail so you will need to restart it.
The reason is our Management-Service and Redirection-Service are not waiting for RabbitMQ/MySQL so they will try to connect before these services are ready.

**ROUTES**:

**Management-Service:**

Create short URL - **POST** http://localhost:8081

Delete URL - **DELETE** http://localhost:8081/{id}

**Redirection-Service**

Redirect - **GET** http://localhost:8082/{hash}
