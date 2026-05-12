const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const Database = require('better-sqlite3');
const { Kafka } = require('kafkajs');

// ─── Database setup ───────────────────────────────────────────────
const db = new Database(path.join(__dirname, 'products.db'));
db.exec(`
  CREATE TABLE IF NOT EXISTS products (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    description TEXT,
    price       REAL NOT NULL,
    stock       INTEGER NOT NULL DEFAULT 0,
    category    TEXT
  )
`);

// ─── Kafka producer ───────────────────────────────────────────────
const kafka = new Kafka({ clientId: 'service-products', brokers: ['localhost:9092'] });
const producer = kafka.producer();

async function startProducer() {
  await producer.connect();
  console.log('Kafka producer connected');
}

// ─── gRPC handlers ────────────────────────────────────────────────
function CreateProduct(call, callback) {
  const { name, description, price, stock, category } = call.request;
  const roundedPrice = Math.round(price * 100) / 100;
  const stmt = db.prepare('INSERT INTO products (name, description, price, stock, category) VALUES (?, ?, ?, ?, ?)');
  const result = stmt.run(name, description, roundedPrice, stock, category);
  const product = db.prepare('SELECT * FROM products WHERE id = ?').get(result.lastInsertRowid);
  callback(null, product);
}

function GetProduct(call, callback) {
  const product = db.prepare('SELECT * FROM products WHERE id = ?').get(call.request.id);
  if (product) {
    callback(null, product);
  } else {
    callback({ code: grpc.status.NOT_FOUND, message: 'Product not found' });
  }
}

function ListProducts(call, callback) {
  const products = db.prepare('SELECT * FROM products').all();
  callback(null, { products });
}

function UpdateStock(call, callback) {
  const { id, stock } = call.request;
  db.prepare('UPDATE products SET stock = ? WHERE id = ?').run(stock, id);
  const product = db.prepare('SELECT * FROM products WHERE id = ?').get(id);
  if (product) {
    // Publish Kafka event
    producer.send({
      topic: 'product.updated',
      messages: [{ value: JSON.stringify({ productId: id, name: product.name, newStock: stock }) }]
    }).catch(console.error);
    callback(null, product);
  } else {
    callback({ code: grpc.status.NOT_FOUND, message: 'Product not found' });
  }
}

function DeleteProduct(call, callback) {
  const result = db.prepare('DELETE FROM products WHERE id = ?').run(call.request.id);
  if (result.changes > 0) {
    callback(null, { success: true, message: 'Product deleted' });
  } else {
    callback(null, { success: false, message: 'Product not found' });
  }
}

// ─── gRPC server ──────────────────────────────────────────────────
async function main() {
  await startProducer();

  const packageDef = protoLoader.loadSync(
    path.join(__dirname, '../proto/product.proto'),
    { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
  );
  const productProto = grpc.loadPackageDefinition(packageDef).product;

  const server = new grpc.Server();
  server.addService(productProto.ProductService.service, {
    CreateProduct,
    GetProduct,
    ListProducts,
    UpdateStock,
    DeleteProduct
  });

  server.bindAsync('0.0.0.0:50052', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); return; }
    console.log(`Products service running on port ${port}`);
  });
}

main().catch(console.error);