const express = require('express');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');
const bodyParser = require('body-parser');
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const app = express();

// ─── Load all .proto files ─────────────────────────────────────────
const PROTO_PATH = path.join(__dirname, '../proto');

const userPackage = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(PROTO_PATH, 'user.proto'), {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
  })
).user;

const productPackage = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(PROTO_PATH, 'product.proto'), {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
  })
).product;

const notifPackage = grpc.loadPackageDefinition(
  protoLoader.loadSync(path.join(PROTO_PATH, 'notification.proto'), {
    keepCase: true, longs: String, enums: String, defaults: true, oneofs: true
  })
).notification;

// ─── gRPC clients ─────────────────────────────────────────────────
const userClient = new userPackage.UserService(
  'localhost:50051', grpc.credentials.createInsecure()
);
const productClient = new productPackage.ProductService(
  'localhost:50052', grpc.credentials.createInsecure()
);
const notifClient = new notifPackage.NotificationService(
  'localhost:50053', grpc.credentials.createInsecure()
);

// ─── Helper: wrap gRPC callback into a Promise ────────────────────
function grpcCall(client, method, payload) {
  return new Promise((resolve, reject) => {
    client[method](payload, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

// ══════════════════════════════════════════════════════════════════
//  REST ENDPOINTS
// ══════════════════════════════════════════════════════════════════

app.use('/api', express.json());

// ── Users ─────────────────────────────────────────────────────────
app.post('/api/users/register', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'RegisterUser', req.body);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post('/api/users/login', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'LoginUser', req.body);
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/users', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'GetAllUsers', {});
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/users/:id', async (req, res) => {
  try {
    const result = await grpcCall(userClient, 'GetUser', { id: parseInt(req.params.id) });
    res.json(result);
  } catch (err) { res.status(404).json({ error: 'User not found' }); }
});

// ── Products ──────────────────────────────────────────────────────
app.get('/api/products', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'ListProducts', {});
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/products/:id', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'GetProduct', { id: parseInt(req.params.id) });
    res.json(result);
  } catch (err) { res.status(404).json({ error: 'Product not found' }); }
});

app.post('/api/products', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'CreateProduct', req.body);
    res.status(201).json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.put('/api/products/:id/stock', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'UpdateStock', {
      id: parseInt(req.params.id),
      stock: req.body.stock
    });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.delete('/api/products/:id', async (req, res) => {
  try {
    const result = await grpcCall(productClient, 'DeleteProduct', { id: parseInt(req.params.id) });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ── Notifications ─────────────────────────────────────────────────
app.get('/api/notifications', async (req, res) => {
  try {
    const result = await grpcCall(notifClient, 'GetNotifications', {});
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/notifications/user/:userId', async (req, res) => {
  try {
    const result = await grpcCall(notifClient, 'GetUserNotifications', {
      user_id: req.params.userId
    });
    res.json(result);
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ══════════════════════════════════════════════════════════════════
//  GRAPHQL
// ══════════════════════════════════════════════════════════════════

const typeDefs = `#graphql
  type User {
    id: Int
    name: String
    email: String
  }

  type Product {
    id: Int
    name: String
    description: String
    price: Float
    stock: Int
    category: String
  }

  type Notification {
    id: String
    type: String
    message: String
    user_id: String
    created_at: String
  }

  type AuthResult {
    success: Boolean
    message: String
    id: Int
    name: String
  }

  type DeleteResult {
    success: Boolean
    message: String
  }

  type Query {
    getUser(id: Int!): User
    getAllUsers: [User]
    getProduct(id: Int!): Product
    listProducts: [Product]
    getNotifications: [Notification]
    getUserNotifications(user_id: String!): [Notification]
  }

  type Mutation {
    registerUser(name: String!, email: String!, password: String!): AuthResult
    loginUser(email: String!, password: String!): AuthResult
    createProduct(name: String!, description: String, price: Float!, stock: Int!, category: String): Product
    updateStock(id: Int!, stock: Int!): Product
    deleteProduct(id: Int!): DeleteResult
  }
`;

const resolvers = {
  Query: {
    getUser: (_, { id }) => grpcCall(userClient, 'GetUser', { id }),
    getAllUsers: () => grpcCall(userClient, 'GetAllUsers', {}).then(r => r.users),
    getProduct: (_, { id }) => grpcCall(productClient, 'GetProduct', { id }),
    listProducts: () => grpcCall(productClient, 'ListProducts', {}).then(r => r.products),
    getNotifications: () => grpcCall(notifClient, 'GetNotifications', {}).then(r => r.notifications),
    getUserNotifications: (_, { user_id }) =>
      grpcCall(notifClient, 'GetUserNotifications', { user_id }).then(r => r.notifications),
  },
  Mutation: {
    registerUser: (_, args) => grpcCall(userClient, 'RegisterUser', args),
    loginUser: (_, args) => grpcCall(userClient, 'LoginUser', args),
    createProduct: (_, args) => grpcCall(productClient, 'CreateProduct', args),
    updateStock: (_, args) => grpcCall(productClient, 'UpdateStock', args),
    deleteProduct: (_, args) => grpcCall(productClient, 'DeleteProduct', args),
  }
};

// ─── Start server ─────────────────────────────────────────────────
async function main() {
  const apollo = new ApolloServer({ typeDefs, resolvers });
  await apollo.start();

app.use('/graphql', express.json(), expressMiddleware(apollo));
  app.listen(3000, () => {
    console.log('');
    console.log('API Gateway running on http://localhost:3000');
    console.log('REST    → http://localhost:3000/api/...');
    console.log('GraphQL → http://localhost:3000/graphql');
    console.log('');
  });
}

main().catch(console.error);