# Marketplace App  Microservices Architecture

A fully functional e-commerce marketplace built with Node.js microservices.

## Architecture

The application is composed of:
- **API Gateway** single entry point (REST + GraphQL)
- **Users Service** handles registration and authentication
- **Products Service**  handles product catalog and stock
- **Notifications Service**  handles event-driven alerts via Kafka
- **Kafka Broker**  async communication between services

## Tech Stack

| Technology | Usage |
|---|---|
| Node.js | All services |
| gRPC + Protobuf | Gateway ↔ Microservices communication |
| REST | Client-facing API |
| GraphQL | Flexible client queries |
| Kafka | Async event streaming |
| SQLite3 | Users and Products databases |
| RxDB | Notifications database (NoSQL) |
| Docker | Kafka + Zookeeper |

## Project Structure

```
marketplace-app/
├── proto/                    
│   ├── user.proto
│   ├── product.proto
│   └── notification.proto
├── gateway/                  
│   └── index.js
├── service-users/            
│   └── index.js
├── service-products/         
│   └── index.js
├── service-notifications/    
│   └── index.js
└── docker-compose.yml        
```

## gRPC Services

### UserService (port 50051)
| Method | Description |
|---|---|
| RegisterUser | Register a new user |
| LoginUser | Authenticate a user |
| GetUser | Get user by ID |
| GetAllUsers | Get all users |

### ProductService (port 50052)
| Method | Description |
|---|---|
| CreateProduct | Add a new product |
| GetProduct | Get product by ID |
| ListProducts | Get all products |
| UpdateStock | Update product stock |
| DeleteProduct | Delete a product |

### NotificationService (port 50053)
| Method | Description |
|---|---|
| GetNotifications | Get all notifications |
| GetUserNotifications | Get notifications by user ID |

## REST Endpoints

### Users
| Method | Endpoint | Description |
|---|---|---|
| POST | /api/users/register | Register new user |
| POST | /api/users/login | Login user |
| GET | /api/users | Get all users |
| GET | /api/users/:id | Get user by ID |

### Products
| Method | Endpoint | Description |
|---|---|---|
| GET | /api/products | List all products |
| GET | /api/products/:id | Get product by ID |
| POST | /api/products | Create product |
| PUT | /api/products/:id/stock | Update stock |
| DELETE | /api/products/:id | Delete product |

### Notifications
| Method | Endpoint | Description |
|---|---|---|
| GET | /api/notifications | Get all notifications |
| GET | /api/notifications/user/:userId | Get user notifications |

## GraphQL Schema

### Queries
```graphql
getAllUsers: [User]
getUser(id: Int!): User
listProducts: [Product]
getProduct(id: Int!): Product
getNotifications: [Notification]
getUserNotifications(user_id: String!): [Notification]
```

### Mutations
```graphql
registerUser(name: String!, email: String!, password: String!): AuthResult
loginUser(email: String!, password: String!): AuthResult
createProduct(name: String!, description: String, price: Float!, stock: Int!, category: String): Product
updateStock(id: Int!, stock: Int!): Product
deleteProduct(id: Int!): DeleteResult
```

## Kafka Topics

| Topic | Producer | Consumer | Trigger |
|---|---|---|---|
| user.registered | service-users | service-notifications | New user registers |
| product.updated | service-products | service-notifications | Stock is updated |

### Message Format

**user.registered:**
```json
{ "userId": 1, "name": "Khouloud", "email": "khouloud@email.com" }
```

**product.updated:**
```json
{ "productId": 1, "name": "Laptop Pro", "newStock": 45 }
```

## Databases

| Service | Database | Type |
|---|---|---|
| service-users | SQLite3 | SQL |
| service-products | SQLite3 | SQL |
| service-notifications | RxDB | NoSQL |

## Installation & Running

### Prerequisites
- Node.js v20+
- Docker Desktop (running)

### Step 1  Install dependencies
```bash
cd gateway && npm install && cd ..
cd service-users && npm install && cd ..
cd service-products && npm install && cd ..
cd service-notifications && npm install && cd ..
```

### Step 2  Start Kafka
```bash
docker compose up -d
```

### Step 3  Start all services (4 separate terminals)
```bash
# Terminal 1
cd service-users && node index.js

# Terminal 2
cd service-products && node index.js

# Terminal 3
cd service-notifications && node index.js

# Terminal 4
cd gateway && node index.js
```

### Step 4  Test the API
- REST: http://localhost:3000/api/products
- GraphQL: http://localhost:3000/graphql
- Postman: https://www.postman.com/lzkhouloud/marketplace-microservices/collection/y2v3gkg/marketplace-api

## Postman Collection
Public collection with all saved requests and responses:
https://www.postman.com/lzkhouloud/marketplace-microservices/collection/y2v3gkg/marketplace-api
