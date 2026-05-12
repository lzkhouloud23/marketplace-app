const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const { Kafka } = require('kafkajs');
const { createRxDatabase, addRxPlugin } = require('rxdb');
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');

// ─── Notification schema for RxDB ─────────────────────────────────
const notificationSchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id:         { type: 'string', maxLength: 100 },
    type:       { type: 'string' },
    message:    { type: 'string' },
    user_id:    { type: 'string' },
    created_at: { type: 'string' }
  },
  required: ['id', 'type', 'message', 'user_id', 'created_at']
};

let notificationsCollection;

async function setupDatabase() {
  const db = await createRxDatabase({ name: 'notificationsdb', storage: getRxStorageMemory() });
  await db.addCollections({ notifications: { schema: notificationSchema } });
  notificationsCollection = db.notifications;
  console.log('RxDB ready');
}

// ─── Kafka consumer ───────────────────────────────────────────────
async function startConsumer() {
  const kafka = new Kafka({ clientId: 'service-notifications', brokers: ['localhost:9092'] });
  const consumer = kafka.consumer({ groupId: 'notifications-group' });
  await consumer.connect();
  await consumer.subscribe({ topics: ['user.registered', 'product.updated'], fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const data = JSON.parse(message.value.toString());
      const notif = {
        id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
        type: topic,
        message: topic === 'user.registered'
          ? `Welcome ${data.name}! Your account has been created.`
          : `Product "${data.name}" stock updated to ${data.newStock}.`,
        user_id: String(data.userId || 'system'),
        created_at: new Date().toISOString()
      };
      await notificationsCollection.insert(notif);
      console.log(`Notification saved: [${topic}]`, notif.message);
    }
  });
  console.log('Kafka consumer listening on user.registered and product.updated');
}

// ─── gRPC handlers ────────────────────────────────────────────────
async function GetNotifications(call, callback) {
  const all = await notificationsCollection.find().exec();
  callback(null, { notifications: all.map(d => d.toJSON()) });
}

async function GetUserNotifications(call, callback) {
  const results = await notificationsCollection
    .find({ selector: { user_id: call.request.user_id } })
    .exec();
  callback(null, { notifications: results.map(d => d.toJSON()) });
}

// ─── gRPC server ──────────────────────────────────────────────────
async function main() {
  await setupDatabase();
  await startConsumer();

  const packageDef = protoLoader.loadSync(
    path.join(__dirname, '../proto/notification.proto'),
    { keepCase: true, longs: String, enums: String, defaults: true, oneofs: true }
  );
  const notifProto = grpc.loadPackageDefinition(packageDef).notification;

  const server = new grpc.Server();
  server.addService(notifProto.NotificationService.service, {
    GetNotifications,
    GetUserNotifications
  });

  server.bindAsync('0.0.0.0:50053', grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) { console.error(err); return; }
    console.log(`Notifications service running on port ${port}`);
  });
}

main().catch(console.error);