const cluster = require("cluster");

//logging//
const winston = require('winston');

const options = {
  file: {
    level: 'info',
    filename:  cluster.isMaster ? '/usr/src/app/master.log' : `/usr/src/app/worker_${cluster.worker.id}.log`,
    handleExceptions: true,
    json: true,
    maxsize: 5242880, // 5MB
    maxFiles: 5,
    colorize: false,
  },
  console: {
    level: 'debug',
    handleExceptions: true,
    json: false,
    colorize: true,
  },
};

const logger = winston.createLogger({
  levels: winston.config.npm.levels,
  transports: [
    new winston.transports.File(options.file),
    new winston.transports.Console(options.console)
  ],
  exitOnError: false
})

//logging end here//


const setupMaster = (httpServer, opts) => {
  if (!cluster.isMaster) {
    throw new Error("not master");
  }

  const options = Object.assign(
    {
      loadBalancingMethod: "least-connection", // either "random", "round-robin" or "least-connection"
    },
    opts
  );

  const sessionIdToWorker = new Map();
  const sidRegex = /sid=([\w\-]{20})/;
  let currentIndex = 0; // for round-robin load balancing

  const computeWorkerId = (data) => {
    const match = sidRegex.exec(data);
    // logger.info(`in computeWorkerId before matching====> data:,`, JSON.stringify(data), `, match: `, match ? match[1] : match, `, sessionIdToWorker: `, sessionIdToWorker);
    if (match) {
      const sid = match[1];
      const workerId = sessionIdToWorker.get(sid);
      // logger.info(`in computeWorkerId, match done 1: ====> sid: ${sid}, worker_id: ${workerId}, workder_ids: `, Object.keys(cluster.workers), `, sessionIdToWorker: `, sessionIdToWorker);
      if (workerId && cluster.workers[workerId]) {
        // logger.info(`in computeWorkerId, match done 2: ====> sid: ${sid}, worker_id: ${workerId}, workder_ids: `, Object.keys(cluster.workers), `, sessionIdToWorker: `, sessionIdToWorker);
      }
      if (workerId && cluster.workers[workerId]) {
        return workerId;
      }
    }
    // logger.info(`computeWorkerId ====> loadBalancingMethod: `, options.loadBalancingMethod, `, sessionIdToWorker: `, sessionIdToWorker);
    switch (options.loadBalancingMethod) {
      case "random": {
        const workerIds = Object.keys(cluster.workers);
        return workerIds[Math.floor(Math.random() * workerIds.length)];
      }
      case "round-robin": {
        const workerIds = Object.keys(cluster.workers);
        currentIndex++;
        if (currentIndex >= workerIds.length) {
          currentIndex = 0;
        }
        return workerIds[currentIndex];
      }
      case "least-connection":
        let leastActiveWorker;
        for (const id in cluster.workers) {
          const worker = cluster.workers[id];
          if (leastActiveWorker === undefined) {
            leastActiveWorker = worker;
          } else {
            const c1 = worker.clientsCount || 0;
            const c2 = leastActiveWorker.clientsCount || 0;
            if (c1 < c2) {
              leastActiveWorker = worker;
            }
          }
        }
        return leastActiveWorker.id;
    }
  };

  httpServer.on("connection", (socket) => {
    socket.once("data", (buffer) => {
      logger.info(`{isMaster: ${cluster.isMaster}} in connection data event, before socket.pause ====> socket_id: ${socket.id}`);
      socket.pause();
      logger.info(`{isMaster: ${cluster.isMaster}} in connection data event, after socket.pause ====> socket_id: ${socket.id}`);
      const data = buffer.toString();
      logger.info(`{isMaster: ${cluster.isMaster}} in connection data event, after socket.pause ====> socket_id: ${socket ? socket.id : 'none'}, data: `, JSON.stringify(data));
      const workerId = computeWorkerId(data);
      logger.info(`{isMaster: ${cluster.isMaster}} in connection data event, after socket.pause ====> socket_id: ${socket ? socket.id : 'none'}, worker_id (from computeWorkerId): ${workerId}`);
      cluster.workers[workerId].send(
        { type: "sticky:connection", data },
        socket,
        (err) => {
          if (err) {
            logger.info(`{isMaster: ${cluster.isMaster}} in connection data event, ERROR, before socket.destroy ====> socket_id: ${socket ? socket.id : 'none'}`, err);
            socket.destroy();
            logger.info(`{isMaster: ${cluster.isMaster}} in connection data event, ERROR, after socket.destroy ====> socket_id: ${socket ? socket.id : 'none'}`, err);
          }
        }
      );
    });
  });

  cluster.on("message", (worker, { type, data }) => {
    switch (type) {
      case "sticky:connection":
        sessionIdToWorker.set(data, worker.id);
        if (options.loadBalancingMethod === "least-connection") {
          worker.clientsCount = (worker.clientsCount || 0) + 1;
        }
        break;
      case "sticky:disconnection":
        sessionIdToWorker.delete(data);
        if (options.loadBalancingMethod === "least-connection") {
          worker.clientsCount--;
        }
        break;
    }
  });
};

const setupWorker = (io) => {
  if (!cluster.isWorker) {
    throw new Error("not worker");
  }

  process.on("message", ({ type, data }, socket) => {//from master to worker
    logger.info(`{ worker_id: ${cluster.worker.id} } received message ====> type: ${type}, socket_id: ${socket.id}, data: `, JSON.stringify(data));
    switch (type) {
      case "sticky:connection":
        if (!socket) {
          // might happen if the socket is closed during the transfer to the worker
          // see https://nodejs.org/api/child_process.html#child_process_example_sending_a_socket_object
          return;
        }
        io.httpServer.emit("connection", socket); // inject connection
        // republish first chunk
        if (socket._handle.onread.length === 1) {
          socket._handle.onread(Buffer.from(data));
        } else {
          // for Node.js < 12
          socket._handle.onread(1, Buffer.from(data));
        }
        socket.resume();
        break;
    }
  });

  const ignoreError = () => {}; // the next request will fail anyway

  io.engine.on("connection", (socket) => {
    process.send({ type: "sticky:connection", data: socket.id }, ignoreError);

    socket.once("close", () => {
      process.send(
        { type: "sticky:disconnection", data: socket.id },
        ignoreError
      );
    });
  });
};

module.exports = {
  setupMaster,
  setupWorker,
};
