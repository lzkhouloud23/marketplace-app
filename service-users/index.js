const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const Database = require('better-sqlite3');
const { Kafka } = require('kafkajs');

// ─── Database setup ───────────────────────────────────────────────
const db = new Database(path.join(__dirname, 'users.db'));
db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name     TEXT NOT NULL,
    email    TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL
  )
`);

// ─── Kafka producer setup ─────────────────────────────────────────
const kafka = new Kafka({ clientId: 'service-users', brokers: ['localhost:9092'] });
const producer = kafka.producer();

async function startProducer() {
  await producer.connect();
  console.log('Kafka producer connected');
}

// ─── gRPC handlers ────────────────────────────────────────────────
function RegisterUser(call, callback) {
  const { name, email, password } = call.request;
  try {
    const stmt = db.prepare('INSERT INTO users (name, email, password) VALUES (?, ?, ?)');
    const result = stmt.run(name, email, password);
    const userId = result.lastInsertRowid;

    // Publish Kafka event
    producer.send({
      topic: 'user.registered',
      messages: [{ value: JSON.stringify({ userId, name, email }) }]
    }).catch(console.error);

    callback(null, { success: true, message: 'User registered successfully', id: userId });
  } catch (err) {
    if (err.message.includes('UNIQUE')) {
      callback(null, { success: false, message: 'Email already exists', id: 0 });
    } else {
      callback(null, { success: false, message: err.message, id: 0 });
    }
  }
}

function LoginUser(call, callback) {
  const { email, password } = call.request;
  const user = db.prepare('SELECT * FROM users WHERE email = ? AND password = ?').get(email, password);
  if (user) {
    callback(null, { success: true, message: 'Login successful', id: user.id, name: user.name });
  } else {
    callback(null, { success: false, message: 'Invalid credentials', id: 0, name: '' });
  }
}

function GetUser(call, callback) {
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(call.request.id);
  if (user) {
    callback(null, { id: user.id, name: user.name, email: user.email });
  } else {
    callback({ code: grpc.status.NOT_FOUND, message: 'User not found' });
  }
}

function GetAllUsers(call, callback) {
  const users = db.prepare('SELECT id, name, email FROM users').all();
  callback(null, { users });
}

// ─── gRPC server ──────────────────────────────────────────────────
async function main() {
  await startProducer();

  const packageDef = protoLoader.loadSync(
    path.join(__dirname, '../proto/user.proto'),
    { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
  );
  const userProto = grpc.loadPackageDefinition(packageDef).user;

  const server = new grpc.Server();
  server.addService(userProto.UserService.service, {
    RegisterUser,
    LoginUser,
    GetUser,
    GetAllUsers
  });

  server.bindAsync('0.0.0.0:50051', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); return; }
    console.log(`Users service running on port ${port}`);
  });
}

main().catch(console.error);